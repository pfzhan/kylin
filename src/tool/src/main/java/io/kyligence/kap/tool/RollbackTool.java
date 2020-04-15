/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.tool;

import static org.apache.kylin.common.util.HadoopUtil.PARQUET_STORAGE_ROOT;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.joda.time.format.DateTimeFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.tool.general.RollbackStatusEnum;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RollbackTool extends ExecutableApplication {

    @SuppressWarnings("static-access")
    private static final String HDFS_METADATA_URL_FORMATTER = "kylin_metadata@hdfs,path=%s";

    private static final Option OPTION_PROJECT = OptionBuilder.hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project level for time travel (optional)").isRequired(false).create("project");

    private static final Option OPTION_SKIP_CHECK_DATA = OptionBuilder.hasArg().withArgName("SKIP_CHECK_DATA")
            .withDescription("Skip check storage data available (optional)").isRequired(false).create("skipCheckData");

    private static final Option OPTION_TIMESTAMP = OptionBuilder.hasArg().withArgName("TIME")
            .withDescription("Specify the travel time(must required)").isRequired(true).create("time");

    private final Options options;
    @VisibleForTesting
    public final KylinConfig kylinConfig;
    private ResourceStore currentResourceStore;

    @VisibleForTesting
    public  RollbackStatusEnum rollbackStatus;

    RollbackTool() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        this.options = new Options();
        initOptions();
    }

    private void initOptions() {
        this.options.addOption(OPTION_PROJECT);
        this.options.addOption(OPTION_TIMESTAMP);
        this.options.addOption(OPTION_SKIP_CHECK_DATA);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    public static void main(String[] args) {
        val tool = new RollbackTool();
        try {
            tool.execute(args);
        } catch (Exception e) {
            log.error("rollback error: {}", e);
            System.exit(1);
        }
        if (tool.rollbackStatus.equals(RollbackStatusEnum.RESTORE_MIRROR_SUCCESS)) {
            System.exit(0);
        } else {
            System.exit(1);
        }

    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        log.info("start roll back");
        log.info("start to init ResourceStore");
        currentResourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        rollbackStatus = RollbackStatusEnum.START;
        if (!checkParam(optionsHelper)) {
            log.error("check param failed");
            return;
        }
        rollbackStatus = RollbackStatusEnum.CHECK_PARAM_SUCCESS;
        log.info("check param success");

        String currentBackupFolder;
        // 1 backup current metadata
        try {
            currentBackupFolder = backupCurrentMetadata(kylinConfig);
        } catch (Exception e) {
            log.error("backup current metadata failed : {}", e);
            return;
        }

        rollbackStatus = RollbackStatusEnum.BACKUP_CURRENT_METADATA_SUCCESS;
        log.info("backup current metadata success");

        // 2 Check whether the current cluster is stopped by checking the port
        if (!checkClusterStatus()) {
            log.error("check cluster status failed");
            return;
        }
        rollbackStatus = RollbackStatusEnum.CHECK_CLUSTER_STATUS_SUCESS;
        log.info("check cluster status success");

        // 3 Get the snapshot file from the backup directory to restore, then replay the auditlog to the user-specified timestamp
        val restoreResourceStore = forwardToTimeStampFromSnapshot(optionsHelper);
        if (restoreResourceStore == null) {
            log.error("forward to timestamp from snapshot failed");
            return;
        }
        rollbackStatus = RollbackStatusEnum.FORWARD_TO_USER_TARGET_TIME_FROM_SNAPSHOT_SUCESS;
        log.info("forward to user target time success");

        // 4 Compare and print metadata differences，remind user
        outputDiff(optionsHelper, currentResourceStore, restoreResourceStore);
        rollbackStatus = RollbackStatusEnum.OUTPUT_DIFF_SUCCESS;
        log.info("output diff success");

        // 5 Waiting for user confirmation
        waitUserConfirm();

        rollbackStatus = RollbackStatusEnum.WAIT_USER_CONFIRM_SUCCESS;
        log.info("wait user confirm success");

        // 6 Check whether the storage data pointed to by the target metadata is available, including cube data, snapshot data, dictionary data, whether the metadata is damaged, etc.
        val skipCheckData = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_SKIP_CHECK_DATA));
        if (!skipCheckData && !checkStorageDataAvailable(optionsHelper, restoreResourceStore)) {
            log.error("target snapshot storage data is unavailable");
            return;
        }
        rollbackStatus = RollbackStatusEnum.CHECK_STORAGE_DATA_AVAILABLE_SUCCESS;
        log.info("check storage data available success");

        // 7 restore target metadata to a current table, if it fails, overwrite it with the current backup metadata
        if (!restoreMirror(optionsHelper.getOptionValue(OPTION_PROJECT), currentResourceStore, restoreResourceStore)) {
            log.error("restore target metadata failed");
            if (restoreCurrentMirror(kylinConfig, currentBackupFolder)) {
                log.error("restore current metadata failed, please restore the metadata database manually "
                        + "the current database backup folder is: backup_current");
            }
            return;
        }
        rollbackStatus = RollbackStatusEnum.RESTORE_MIRROR_SUCCESS;
        log.info("restore mirror success");

        log.info("roll back success");
    }

    private Boolean checkParam(OptionsHelper optionsHelper) {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        val userTargetTime = optionsHelper.getOptionValue(OPTION_TIMESTAMP);
        if (userTargetTime == null) {
            log.error("specify project level time travel (must required)");
            return false;
        }
        try {
            formatter.parseDateTime(userTargetTime);
        } catch (Exception e) {
            log.error("parse user specified time failed {}", e);
            return false;
        }

        long userTargetTimeMillis = formatter.parseDateTime(userTargetTime).getMillis();
        long protectionTime = System.currentTimeMillis() - kylinConfig.getStorageResourceSurvivalTimeThreshold();
        if (userTargetTimeMillis < protectionTime) {
            log.error("user specified time  is less than protection time");
            return false;
        }

        return true;
    }

    @VisibleForTesting
    public Boolean waitUserConfirm() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            log.info("please enter:understand the impact and confirm the implementation");
            String line = scanner.nextLine();
            if (line.equals("understand the impact and confirm the implementation")) {
                break;
            }
        }
        return true;
    }

    private Boolean exists(FileSystem fs, Path path) {
        try {
            val exist = fs.exists(path);
            if (!exist) {
                log.error("check file path: {} failed", path);
            }
            return exist;
        } catch (Exception e) {
            log.error("check file path: {} failed : {}", path, e);
            return false;
        }
    }

    @VisibleForTesting
    public Boolean checkClusterStatus() {
        return isPortAvailable(Integer.valueOf(kylinConfig.getServerPort()));
    }

    private boolean isPortAvailable(int port) {
        try (ServerSocket server = new ServerSocket(port)) {
            log.info("The port : {} is available", port);
            return true;
        } catch (IOException e) {
            log.error("dectect port available failed: {}", e);
        }
        return false;
    }

    private void outputDiff(String tag, Set<String> origin, Set<String> target) {
        val willBeDeleted = Sets.difference(origin, target);
        if (!willBeDeleted.isEmpty()) {
            log.info("{} {} will be deleted", tag, willBeDeleted.toString());
        }
        val willbeAdded = Sets.difference(target, origin);
        if (!willbeAdded.isEmpty()) {
            log.info("{} {} will be added", tag, willbeAdded);
        }
    }

    private void outputDiff(OptionsHelper optionsHelper, ResourceStore currentResourceStore,
            ResourceStore restoreResourceStore) throws IOException {

        String targetProject = optionsHelper.getOptionValue(OPTION_PROJECT);
        KylinConfig currentConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore.setRS(currentConfig, currentResourceStore);

        KylinConfig restoreConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore.setRS(restoreConfig, restoreResourceStore);

        Set<String> updateProjects;
        if (StringUtils.isBlank(targetProject)) {
            val currentAclManager = NKylinUserManager.getInstance(currentConfig);
            val restoreAclManager = NKylinUserManager.getInstance(restoreConfig);
            val currentUsers = currentAclManager.list().stream().map(ManagedUser::getUsername)
                    .collect(Collectors.toSet());
            val restoreUsers = restoreAclManager.list().stream().map(ManagedUser::getUsername)
                    .collect(Collectors.toSet());
            outputDiff("user:", currentUsers, restoreUsers);
            val currentProjectMgr = NProjectManager.getInstance(currentConfig);
            val restoreProjectMgr = NProjectManager.getInstance(restoreConfig);

            val currentProjects = currentProjectMgr.listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toSet());
            val restoreProjects = restoreProjectMgr.listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toSet());
            outputDiff("project: ", currentProjects, restoreProjects);
            updateProjects = Sets.intersection(currentProjects, restoreProjects);
        } else {
            updateProjects = Sets.newHashSet(targetProject);
        }

        for (String project : updateProjects) {

            val currentExecutableManager = NExecutableManager.getInstance(currentConfig, project);
            val restoreExecutableManager = NExecutableManager.getInstance(restoreConfig, project);

            restoreExecutableManager.getAllExecutables().stream().forEach(e -> {
                if (currentExecutableManager.getJob(e.getId()) != null) {
                    val currentStatus = currentExecutableManager.getOutput(e.getId()).getState();
                    val restoreStatus = restoreExecutableManager.getOutput(e.getId()).getState();
                    if (currentStatus.isFinalState() && restoreStatus.isProgressing()) {
                        log.info("job : {} will be re-executed and will  affect segments: {}", e,
                            Sets.newHashSet(e.getTargetSegments()).toString());
                    }
                }
            });

            val currentDfMgr = NDataflowManager.getInstance(currentConfig, project);
            val restoreDfMgr = NDataflowManager.getInstance(restoreConfig, project);
            val currentModelMgr = NDataModelManager.getInstance(currentConfig, project);
            val restoreModelMgr = NDataModelManager.getInstance(restoreConfig, project);

            val currentDataflows = currentDfMgr.listAllDataflows();
            val restoreDataflows = restoreDfMgr.listAllDataflows();

            val currentModels = currentDataflows.stream().map(NDataflow::getModel).map(model -> model.getAlias())
                    .collect(Collectors.toSet());
            val restoreModels = restoreDataflows.stream().map(NDataflow::getModel).map(model -> model.getAlias())
                    .collect(Collectors.toSet());

            outputDiff("model: ", currentModels, restoreModels);

            val updateModels = Sets.intersection(currentModels, restoreModels);

            for (String model : updateModels) {
                currentModelMgr.getDataModelDescByAlias(model).toString();
                val currentNamedColumns = currentModelMgr.getDataModelDescByAlias(model).getAllNamedColumns().stream()
                        .map(m -> String.valueOf(m.getId()) + m.getName()).collect(Collectors.toSet());
                val restoreNamedColumns = restoreModelMgr.getDataModelDescByAlias(model).getAllNamedColumns().stream()
                        .map(m -> String.valueOf(m.getId()) + m.getName()).collect(Collectors.toSet());
                outputDiff("named columns: ", currentNamedColumns, restoreNamedColumns);

                val currentSegments = currentDfMgr.getDataflowByModelAlias(model).getSegments().stream()
                        .map(NDataSegment::toString).collect(Collectors.toSet());
                val restoreSegments = restoreDfMgr.getDataflowByModelAlias(model).getSegments().stream()
                        .map(NDataSegment::toString).collect(Collectors.toSet());

                outputDiff("segments: ", currentSegments, restoreSegments);
            }
        }
    }

    private Boolean checkStorageDataAvailable(OptionsHelper optionsHelper, ResourceStore restoreResourceStore) {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);

        KylinConfig configCopy = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore.setRS(configCopy, restoreResourceStore);
        val fs = HadoopUtil.getWorkingFileSystem();

        NProjectManager prMgr = NProjectManager.getInstance(configCopy);

        val projects = project == null ? prMgr.listAllProjects() : Lists.newArrayList(prMgr.getProject(project));
        var status = true;
        for (ProjectInstance p : projects) {
            if (!checkProjectStorageDataAvailable(configCopy, fs, p.getName())) {
                log.info("project: {} check storage data available failed", p.getName());
                status = false;
            } else {
                log.info("project: {} check storage data available success", p.getName());
            }
            if (!status) {
                log.error("check check storage data available failed");
                return false;
            }
        }
        return true;
    }

    private Boolean checkProjectStorageDataAvailable(KylinConfig config, FileSystem fs, String project) {

        val hdfsWorkingDir = KapConfig.getInstanceFromEnv().getReadHdfsWorkingDirectory();

        NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        val activeIndexDataPath = Sets.<String> newHashSet();
        dfMgr.listAllDataflows()
                .forEach(dataflow -> dataflow.getSegments().stream()
                        .flatMap(segment -> segment.getLayoutsMap().values().stream()).map(this::getDataLayoutDir)
                        .forEach(activeIndexDataPath::add));

        if (!activeIndexDataPath.stream().allMatch(e -> exists(fs, new Path(hdfsWorkingDir, e)))) {
            log.error("check all index file exist failed");
            return false;
        }

        val tableManager = NTableMetadataManager.getInstance(config, project);
        if (!tableManager.listAllTables().stream().map(table -> table.getLastSnapshotPath()).filter(p -> p != null)
                .allMatch(e -> exists(fs, new Path(hdfsWorkingDir, e)))) {
            log.error("check all table snapshot path failed");
            return false;
        }
        return true;
    }

    private String getDataLayoutDir(NDataLayout dataLayout) {
        NDataSegDetails segDetails = dataLayout.getSegDetails();
        return getDataflowDir(segDetails.getProject(), segDetails.getDataSegment().getDataflow().getId()) + "/"
                + segDetails.getUuid() + "/" + dataLayout.getLayoutId();
    }

    private String getDataflowBaseDir(String project) {
        return project + PARQUET_STORAGE_ROOT + "/";
    }

    private String getDataflowDir(String project, String dataflowId) {
        return getDataflowBaseDir(project) + dataflowId;
    }

    @VisibleForTesting
    public Boolean restoreMirror(String project, ResourceStore currentResourceStore,
            ResourceStore restoreResourceStore) {
        try {
            MetadataChecker metadataChecker = new MetadataChecker(restoreResourceStore.getMetadataStore());
            val verifyResult = metadataChecker.verify();
            if (!verifyResult.isQualified()) {
                log.error("{} \n the metadata dir is not qualified", verifyResult.getResultMessage());
            }

            MetadataTool.restore(currentResourceStore, restoreResourceStore, project);
        } catch (Exception e) {
            log.error("restore mirror resource store failed: {} ", e);
        }
        return true;
    }

    private ResourceStore forwardToTimeStampFromSnapshot(OptionsHelper optionsHelper) throws IOException {
        val fs = HadoopUtil.getWorkingFileSystem();
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val path = HadoopUtil.getBackupFolder(kylinConfig);
        if (!exists(fs, new Path(path))) {
            log.error("check default backup folder failed");
            return null;
        }

        val userTimeformatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        val userTargetTime = userTimeformatter.parseDateTime(optionsHelper.getOptionValue(OPTION_TIMESTAMP))
                .getMillis();

        if (userTargetTime > System.currentTimeMillis()) {
            log.error("User-specified time cannot be greater than the current time");
            return null;
        }

        // Select last folder less than user specified time
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");
        val candidateFolder = Arrays.stream(fs.listStatus(new Path(path))).filter(fileStatus -> {
            return Pattern.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}_backup", fileStatus.getPath().getName());
        }).filter(fileStatus -> {
            val filePrefix = fileStatus.getPath().getName().substring(0, "yyyy-MM-dd-HH-mm-ss".length());
            return formatter.parseDateTime(filePrefix).getMillis() < Long.valueOf(userTargetTime);
        }).collect(Collectors.toSet());

        if (candidateFolder.isEmpty()) {
            log.error("check default backup folder failed");
            return null;
        }

        val last = candidateFolder.stream().max(Comparator.comparingLong(FileStatus::getModificationTime));

        val folder = last.get().getPath().getName();
        val restorePath = StringUtils.appendIfMissing(path, "/") + folder;
        val restoreMetadataUrl = getMetadataUrl(restorePath, false);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);

        log.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, restorePath);

        val restoreResourceStore = ResourceStore.getKylinMetaStore(restoreConfig);

        val imageDesc = JsonUtil.readValue(
                restoreResourceStore.getResource(ResourceStore.METASTORE_IMAGE).getByteSource().read(),
                ImageDesc.class);
        val offset = imageDesc.getOffset();
        val auditlog = currentResourceStore.getMetadataStore().getAuditLogStore();

        // replay the resourceStore restored from backup to the latest
        val currentAuditlog = currentResourceStore.getAuditLogStore();
        var startId = offset;
        val maxId = currentAuditlog.getMaxId();
        val minId = currentAuditlog.getMinId();

        //startId +1 is because if there is no data in the database and it is backed up once, here the startId is 0, if a piece of data is inserted, the minId here is 1
        if (startId + 1 < minId) {
            log.error("backup offset is less than  auditlog smallest id");
            return null;
        }
        if (startId > maxId) {
            log.error("backup offset is greater than auditlog largest  id");
            return null;
        }
        val step = 1000L;
        val unitIds = Sets.newHashSet();
        while (startId < maxId) {

            val logs = currentAuditlog.fetch(startId, Math.min(step, maxId - startId));
            for (AuditLog log : logs) {
                //Ensure that all logs involved in transactions must be replayed
                if (log.getTimestamp() >= Long.valueOf(userTargetTime) && !unitIds.contains(log.getUnitId())) {
                    continue;
                }
                unitIds.add(log.getUnitId());
                val event = Event.fromLog(log);
                if (event instanceof ResourceCreateOrUpdateEvent) {
                    restoreResourceStore.deleteResource(log.getResPath());
                    restoreResourceStore.putResourceWithoutCheck(log.getResPath(), log.getByteSource(),
                            log.getTimestamp(), log.getMvcc());
                } else if (event instanceof ResourceDeleteEvent) {
                    restoreResourceStore.deleteResource(log.getResPath());
                }
            }
            startId += step;
        }
        //If the recovered item does not exist in the shuttle version, fail
        try {
            if (!StringUtils.isBlank(project) && restoreResourceStore.listResourcesRecursively("/" + project) == null) {
                log.error("restore project: {} is have not exist in user specified time", project);
                return null;
            }
        } catch (Exception e) {
            log.error("list project: {} error: {}", project, e);
            return null;
        }
        return restoreResourceStore;
    }

    private String backupCurrentMetadata(KylinConfig kylinConfig) throws Exception {
        val currentBackupFolder = LocalDateTime.now().format(MetadataTool.DATE_TIME_FORMATTER) + "_backup";
        MetadataTool.backup(kylinConfig, kylinConfig.getHdfsWorkingDirectory() + "_current_backup",
                currentBackupFolder);
        return currentBackupFolder;
    }

    private Boolean restoreCurrentMirror(KylinConfig kylinConfig, String currentBackupFolder) throws Exception {
        val restoreFolder = kylinConfig.getHdfsWorkingDirectory() + "_current_backup" + File.separator
                + currentBackupFolder;
        try {
            MetadataTool.restore(kylinConfig, restoreFolder);
        } catch (Exception e) {
            log.error("restore current mirror back failed: {} Please use MetadataTool to "
                    + "restore the current backup manually. The backup directory is in {}", e, restoreFolder);
        }
        return true;
    }

    String getMetadataUrl(String rootPath, boolean compressed) {
        if (rootPath.startsWith("hdfs://")) {
            val url = String.format(HDFS_METADATA_URL_FORMATTER,
                    Path.getPathWithoutSchemeAndAuthority(new Path(rootPath)).toString() + "/");
            return compressed ? url + ",zip=1" : url;

        } else if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return StringUtils.appendIfMissing(rootPath, "/");

        } else {
            return StringUtils.appendIfMissing(rootPath, "/");

        }
    }
}
