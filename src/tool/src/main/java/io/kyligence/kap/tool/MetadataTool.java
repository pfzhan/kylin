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

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataTool extends ExecutableApplication {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private static final String HDFS_METADATA_URL_FROMATTER = "kylin_metadata@hdfs,path=%s";

    private static final int RESTORE_FAILED = 11;
    private static final int BACKUP_FAILED = 12;
    private static final int LOCAL_BACKUP_SUCCEED = 1;
    private static final int REMOTE_BACKUP_SUCCEED = 2;
    private static final int RESTORE_SUCCEED = 3;

    @SuppressWarnings("static-access")
    private static final Option OPERATE_BACKUP = OptionBuilder
            .withDescription("Backup metadata to local path or HDFS path").isRequired(false).create("backup");

    private static final Option OPERATE_RESTORE = OptionBuilder
            .withDescription("Restore metadata from local path or HDFS path").isRequired(false).create("restore");

    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(true).create("dir");

    private static final Option OPTION_PROJECT = OptionBuilder.hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project level backup and restore (optional)").isRequired(false).create("project");

    private static final Option FOLDER_NAME = OptionBuilder.hasArg().withArgName("FOLDER_NAME")
            .withDescription("Specify the folder name for backup").isRequired(false).create("folder");

    private final Options options;

    private final KylinConfig kylinConfig;

    private ResourceStore resourceStore;

    MetadataTool() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        this.options = new Options();
        initOptions();
    }

    public MetadataTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    private void initOptions() {
        final OptionGroup optionGroup = new OptionGroup();
        optionGroup.setRequired(true);
        optionGroup.addOption(OPERATE_BACKUP);
        optionGroup.addOption(OPERATE_RESTORE);

        options.addOptionGroup(optionGroup);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_PROJECT);
        options.addOption(FOLDER_NAME);
    }

    public static void main(String[] args) {
        val tool = new MetadataTool();

        int retFlag;
        boolean isBackup = true;
        OptionsHelper optionsHelper = new OptionsHelper();
        try (val curatorOperator = new CuratorOperator()) {
            optionsHelper.parseOptions(tool.getOptions(), args);
            isBackup = isBackupOption(optionsHelper);
            if (!curatorOperator.isJobNodeExist()) {
                tool.execute(args);
                retFlag = isBackup ? LOCAL_BACKUP_SUCCEED : RESTORE_SUCCEED;
            } else {
                if (!isBackup) {
                    log.warn("Fail to restore, please stop all job nodes first");
                    retFlag = RESTORE_FAILED;
                } else {
                    val address = curatorOperator.getAddress();
                    log.info("found a job node running, backup will be delegated to it at server: {}, this may be a remote server.", address);
                    val ret = remoteBackup(address, optionsHelper);
                    if ("000".equals(ret.get("code"))) {
                        log.info("backup successfully at {}", optionsHelper.getOptionValue(OPTION_DIR));
                        retFlag = REMOTE_BACKUP_SUCCEED;
                    } else {
                        log.error("backup failed");
                        retFlag = BACKUP_FAILED;
                    }
                }
            }

        } catch (Exception e) {
            log.error("", e);
            retFlag = isBackup? BACKUP_FAILED : RESTORE_FAILED;
        }
        System.exit(retFlag);
    }

    private static Map remoteBackup(String address, OptionsHelper optionsHelper) throws Exception {
        val restTemplate = new RestTemplate();

        Map<String, String> map = new HashMap<>();
        map.put("backup_path", optionsHelper.getOptionValue(MetadataTool.OPTION_DIR));
        if (optionsHelper.hasOption(MetadataTool.OPTION_PROJECT)) {
            map.put("project", optionsHelper.getOptionValue(MetadataTool.OPTION_PROJECT));
        }
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] body = objectMapper.writeValueAsBytes(map);

        HttpHeaders headers = new HttpHeaders();
        headers.put(HttpHeaders.CONTENT_TYPE, Lists.newArrayList("application/vnd.apache.kylin-v2+json"));

        val response = restTemplate.postForEntity("http://" + address + "/kylin/api/system/backup",
                new HttpEntity<>(body, headers), Map.class);
        return response.getBody();
    }

    private static boolean isBackupOption(OptionsHelper optionsHelper) {
        return optionsHelper.hasOption(OPERATE_BACKUP);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        log.info("start to init ResourceStore");
        resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            boolean isGlobal = null == optionsHelper.getOptionValue(OPTION_PROJECT);
            long startAt = System.currentTimeMillis();

            try {
                backup(optionsHelper);
            } catch (Exception be) {
                if (isGlobal) {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.GLOBAL, "global");
                } else {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT));
                }
                throw be;
            } finally {
                if (isGlobal) {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP, NMetricsCategory.GLOBAL, "global");
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.GLOBAL, "global",
                            System.currentTimeMillis() - startAt);
                } else {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP, NMetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT));
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT), System.currentTimeMillis() - startAt);
                }
            }

        } else if (optionsHelper.hasOption(OPERATE_RESTORE)) {
            restore(optionsHelper);
        } else {
            throw new IllegalArgumentException("The input parameters are wrong");
        }
    }

    private void backup(OptionsHelper optionsHelper) throws Exception {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val path = optionsHelper.getOptionValue(OPTION_DIR);
        var folder = optionsHelper.getOptionValue(FOLDER_NAME);
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now().format(DATE_TIME_FORMATTER) + "_backup";
        }
        val backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        val backupMetadataUrl = getMetadataUrl(backupPath);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        val fs = HadoopUtil.getFileSystem(backupPath);
        fs.delete(new Path(backupPath), true);
        log.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);

        val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig);
        val backupMetadataStore = backupResourceStore.getMetadataStore();

        if (StringUtils.isBlank(project)) {
            log.info("start to copy all projects from ResourceStore.");

            UnitOfAllWorks.doInTransaction(() -> {
                val projectFolders = resourceStore.listResources("/");
                for (String projectPath : projectFolders) {
                    if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)) {
                        continue;
                    }
                    // The "_global" directory is already included in the full backup
                    copyResourceStore(projectPath, resourceStore, backupResourceStore, false);
                }
                val auditLogStore = resourceStore.getAuditLogStore();
                val offset = auditLogStore.getMaxId();
                backupMetadataStore.putResource(new RawResource(ResourceStore.METASTORE_IMAGE,
                        ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(new ImageDesc(offset))),
                        System.currentTimeMillis(), -1));
                return null;
            }, true);
            log.info("start to backup all projects");

        } else {
            log.info("start to copy project {} from ResourceStore.", project);
            UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.builder().readonly(true).unitName(project).processor(() -> {
                        copyResourceStore("/" + project, resourceStore, backupResourceStore, true);
                        return null;
                    }).build());

            log.info("start to backup project {}", project);
        }

        backupMetadataStore.dump(backupResourceStore);
        log.info("backup successfully at {}", path);
    }

    public void copyResourceStore(String projectPath, ResourceStore srcResourceStore, ResourceStore destResourceStore,
            boolean isProjectLevel) {
        srcResourceStore.copy(projectPath, destResourceStore);
        if (isProjectLevel) {
            // The project-level backup needs to contain "/_global/project/*.json"
            val projectName = Paths.get(projectPath).getFileName().toString();
            srcResourceStore.copy(ProjectInstance.concatResourcePath(projectName), destResourceStore);
        }
    }

    private void restore(OptionsHelper optionsHelper) throws IOException {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val restorePath = optionsHelper.getOptionValue(OPTION_DIR);

        val restoreMetadataUrl = getMetadataUrl(restorePath);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);
        log.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, restorePath);

        val restoreResourceStore = ResourceStore.getKylinMetaStore(restoreConfig);
        val restoreMetadataStore = restoreResourceStore.getMetadataStore();

        val verifyResult = restoreMetadataStore.verify();
        if (!verifyResult.isQualified()) {
            throw new RuntimeException(verifyResult.getResultMessage() + "\n the metadata dir is not qualified");
        }

        if (StringUtils.isBlank(project)) {
            log.info("start to restore all projects");
            val srcProjectFolders = restoreResourceStore.listResources("/");
            val destProjectFolders = resourceStore.listResources("/");
            val projectFolders = Sets.union(srcProjectFolders, destProjectFolders);

            for (String projectPath : projectFolders) {
                if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)
                        || projectPath.equals(ResourceStore.METASTORE_IMAGE)) {
                    continue;
                }
                val projectName = Paths.get(projectPath).getName(0).toString();
                val destResources = resourceStore.listResourcesRecursively(projectPath);
                val srcResources = restoreMetadataStore.list(projectPath).stream().map(x -> projectPath + x)
                        .collect(Collectors.toSet());
                UnitOfWork.doInTransactionWithRetry(() -> doRestore(restoreMetadataStore, destResources, srcResources),
                        projectName);
            }

        } else {
            log.info("start to restore project {}", project);
            val globalDestResources = resourceStore.listResourcesRecursively(ResourceStore.PROJECT_ROOT).stream()
                    .filter(x -> Paths.get(x).getFileName().toString().startsWith(project)).collect(Collectors.toSet());
            val globalSrcResources = restoreMetadataStore.list(ResourceStore.PROJECT_ROOT).stream()
                    .filter(x -> Paths.get(x).getFileName().toString().startsWith(project))
                    .map(x -> ResourceStore.PROJECT_ROOT + x).collect(Collectors.toSet());
            UnitOfWork.doInTransactionWithRetry(
                    () -> doRestore(restoreMetadataStore, globalDestResources, globalSrcResources),
                    UnitOfWork.GLOBAL_UNIT);

            val projectPath = "/" + project;
            val destResources = resourceStore.listResourcesRecursively(projectPath);
            val srcResources = restoreMetadataStore.list(projectPath).stream().map(x -> projectPath + x)
                    .collect(Collectors.toSet());
            UnitOfWork.doInTransactionWithRetry(() -> doRestore(restoreMetadataStore, destResources, srcResources),
                    project);
        }

        log.info("restore successfully");

        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(kylinConfig) };
        val backupTool = new MetadataTool();
        backupTool.execute(args);
    }

    private int doRestore(MetadataStore restoreMetadataStore, Set<String> destResources, Set<String> srcResources)
            throws IOException {
        val threadViewRS = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        //check destResources and srcResources are null,because  Sets.difference(srcResources, destResources) will report NullPointerException
        destResources = destResources == null ? Collections.EMPTY_SET : destResources;
        srcResources = srcResources == null ? Collections.EMPTY_SET : srcResources;

        val insertRes = Sets.difference(srcResources, destResources);
        for (val res : insertRes) {
            val metadataRaw = restoreMetadataStore.load(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), -1L);
        }

        val updateRes = Sets.intersection(destResources, srcResources);
        for (val res : updateRes) {
            val raw = resourceStore.getResource(res);
            val metadataRaw = restoreMetadataStore.load(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), raw.getMvcc());
        }

        val deleteRes = Sets.difference(destResources, srcResources);
        for (val res : deleteRes) {
            threadViewRS.deleteResource(res);
        }

        return 0;
    }

    private String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("hdfs://")) {
            return String.format(HDFS_METADATA_URL_FROMATTER,
                    Path.getPathWithoutSchemeAndAuthority(new Path(rootPath)).toString() + "/");

        } else if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return StringUtils.appendIfMissing(rootPath, "/");

        } else {
            return StringUtils.appendIfMissing(rootPath, "/");

        }
    }
}