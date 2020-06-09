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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.tool.util.ServiceDiscoveryUtil.runWithCurator;
import static org.apache.kylin.tool.ToolErrorCode.FILE_ALREADY_EXIST;
import static org.apache.kylin.tool.ToolErrorCode.INVALID_SHELL_PARAMETER;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import lombok.val;
import lombok.var;

public class MetadataTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private static final String HDFS_METADATA_URL_FORMATTER = "kylin_metadata@hdfs,path=%s";

    private static final String GLOBAL = "global";

    @SuppressWarnings("static-access")
    private static final Option OPERATE_BACKUP = OptionBuilder
            .withDescription("Backup metadata to local path or HDFS path").isRequired(false).create("backup");

    private static final Option OPERATE_COMPRESS = OptionBuilder
            .withDescription("Backup compressed metadata to HDFS path").isRequired(false).create("compress");

    private static final Option OPERATE_RESTORE = OptionBuilder
            .withDescription("Restore metadata from local path or HDFS path").isRequired(false).create("restore");

    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(false).create("dir");

    private static final Option OPTION_PROJECT = OptionBuilder.hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project level backup and restore (optional)").isRequired(false).create("project");

    private static final Option FOLDER_NAME = OptionBuilder.hasArg().withArgName("FOLDER_NAME")
            .withDescription("Specify the folder name for backup").isRequired(false).create("folder");

    private static final Option OPTION_EXCLUDE_TABLE_EXD = OptionBuilder
            .withDescription("Exclude metadata {project}/table_exd directory").isRequired(false)
            .create("excludeTableExd");

    private final Options options;

    private final KylinConfig kylinConfig;

    private ResourceStore resourceStore;

    MetadataTool() {
        kylinConfig = KylinConfig.newKylinConfig();
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
        options.addOption(OPERATE_COMPRESS);
        options.addOption(OPTION_EXCLUDE_TABLE_EXD);
    }

    public static void backup(KylinConfig kylinConfig) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", HadoopUtil.getBackupFolder(kylinConfig) };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void backup(KylinConfig kylinConfig, String dir, String folder) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", dir, "-folder", folder };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void restore(KylinConfig kylinConfig, String folder) throws IOException {
        val tool = new MetadataTool(kylinConfig);
        tool.execute(new String[] { "-restore", "-dir", folder });
    }

    public static void main(String[] args) {
        val tool = new MetadataTool(KylinConfig.getInstanceFromEnv());

        int retFlag = runWithCurator((isLocal, address) -> {
            val optionsHelper = new OptionsHelper();
            optionsHelper.parseOptions(tool.getOptions(), args);
            boolean isBackup = optionsHelper.hasOption(OPERATE_BACKUP);
            if (isLocal || isBackup) {
                tool.execute(args);
                return 0;
            }

            if (!isBackup) {
                logger.warn("Fail to restore, please stop all job nodes first");
                return 1;
            }
            logger.info(
                    "found a job node running, backup will be delegated to it at server: {}, this may be a remote server.",
                    address);
            val ret = tool.remoteBackup(address, optionsHelper.getOptionValue(OPTION_DIR),
                    optionsHelper.getOptionValue(OPTION_PROJECT), optionsHelper.hasOption(OPERATE_COMPRESS));
            if ("000".equals(ret.get("code"))) {
                logger.info("backup successfully at {}", optionsHelper.getOptionValue(OPTION_DIR));
                return 0;
            } else {
                logger.error("backup failed, response is {}", ret);
                return 1;
            }
        });
        System.exit(retFlag);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("start to init ResourceStore");
        resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            boolean isGlobal = null == optionsHelper.getOptionValue(OPTION_PROJECT);
            long startAt = System.currentTimeMillis();

            try {
                backup(optionsHelper);
            } catch (Exception be) {
                if (isGlobal) {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.GLOBAL, GLOBAL);
                } else {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT));
                }
                throw be;
            } finally {
                if (isGlobal) {
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP, NMetricsCategory.GLOBAL, GLOBAL);
                    NMetricsGroup.counterInc(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.GLOBAL, GLOBAL,
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
            throw new KylinException(INVALID_SHELL_PARAMETER, "The input parameters are wrong");
        }
    }

    private void abortIfAlreadyExists(String path) throws IOException {
        URI uri = HadoopUtil.makeURI(path);
        if (!uri.isAbsolute()) {
            logger.info("no scheme specified for {}, try local file system file://", path);
            File localFile = new File(path);
            if (localFile.exists()) {
                logger.error("[UNEXPECTED_THINGS_HAPPENED] local file {} already exists ", path);
                throw new KylinException(FILE_ALREADY_EXIST, path);
            }
            return;
        }
        val fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(new Path(path))) {
            logger.error("[UNEXPECTED_THINGS_HAPPENED] specified file {} already exists ", path);
            throw new KylinException(FILE_ALREADY_EXIST, path);
        }
    }

    private void backup(OptionsHelper optionsHelper) throws Exception {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        var path = optionsHelper.getOptionValue(OPTION_DIR);
        var folder = optionsHelper.getOptionValue(FOLDER_NAME);
        var compress = optionsHelper.hasOption(OPERATE_COMPRESS);
        val excludeTableExd = optionsHelper.hasOption(OPTION_EXCLUDE_TABLE_EXD);
        if (StringUtils.isBlank(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "meta_backups";
        }
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now().format(DATE_TIME_FORMATTER) + "_backup";
        }
        val backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        val backupMetadataUrl = getMetadataUrl(backupPath, compress);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        abortIfAlreadyExists(backupPath);
        logger.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);

        val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig);

        val backupMetadataStore = backupResourceStore.getMetadataStore();

        if (StringUtils.isBlank(project)) {
            logger.info("start to copy all projects from ResourceStore.");

            UnitOfAllWorks.doInTransaction(() -> {
                var projectFolders = resourceStore.listResources("/");
                if (projectFolders == null) {
                    return null;
                }
                for (String projectPath : projectFolders) {
                    if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)
                            || projectPath.equals(ResourceStore.METASTORE_IMAGE)) {
                        continue;
                    }
                    // The "_global" directory is already included in the full backup
                    copyResourceStore(projectPath, resourceStore, backupResourceStore, false, excludeTableExd);
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("metadata task is interrupt");
                    }
                }
                val auditLogStore = resourceStore.getAuditLogStore();
                val offset = auditLogStore.getMaxId();
                backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_IMAGE,
                        ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(new ImageDesc(offset))),
                        System.currentTimeMillis(), -1);
                val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                if (uuid != null) {
                    backupResourceStore.putResourceWithoutCheck(uuid.getResPath(), uuid.getByteSource(),
                            uuid.getTimestamp(), -1);
                }
                return null;
            }, true);
            logger.info("start to backup all projects");

        } else {
            logger.info("start to copy project {} from ResourceStore.", project);
            UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.builder().readonly(true).unitName(project).processor(() -> {
                        copyResourceStore("/" + project, resourceStore, backupResourceStore, true, excludeTableExd);
                        val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                        backupResourceStore.putResourceWithoutCheck(uuid.getResPath(), uuid.getByteSource(),
                                uuid.getTimestamp(), -1);
                        return null;
                    }).build());
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("metadata task is interrupt");
            }
            logger.info("start to backup project {}", project);
        }
        backupResourceStore.deleteResource(ResourceStore.METASTORE_TRASH_RECORD);
        backupMetadataStore.dump(backupResourceStore);
        logger.info("backup successfully at {}", path);
    }

    private Map remoteBackup(String address, String backupPath, String project, boolean compress) throws Exception {
        val restTemplate = new RestTemplate();

        Map<String, String> map = new HashMap<>();
        map.put("backup_path", backupPath);
        map.put("project", project);
        map.put("compress", compress + "");
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] body = objectMapper.writeValueAsBytes(map);

        HttpHeaders headers = new HttpHeaders();
        headers.put(HttpHeaders.CONTENT_TYPE, Lists.newArrayList(HTTP_VND_APACHE_KYLIN_JSON));

        val response = restTemplate.postForEntity("http://" + address + "/kylin/api/system/backup",
                new HttpEntity<>(body, headers), Map.class);
        return response.getBody();
    }

    private void copyResourceStore(String projectPath, ResourceStore srcResourceStore, ResourceStore destResourceStore,
            boolean isProjectLevel, boolean excludeTableExd) {
        if (excludeTableExd) {
            String tableExdPath = projectPath + ResourceStore.TABLE_EXD_RESOURCE_ROOT;
            var projectItems = srcResourceStore.listResources(projectPath);
            for (String item : projectItems) {
                if (item.equals(tableExdPath)) {
                    continue;
                }
                srcResourceStore.copy(item, destResourceStore);
            }
        } else {
            srcResourceStore.copy(projectPath, destResourceStore);
        }
        if (isProjectLevel) {
            // The project-level backup needs to contain "/_global/project/*.json"
            val projectName = Paths.get(projectPath).getFileName().toString();
            srcResourceStore.copy(ProjectInstance.concatResourcePath(projectName), destResourceStore);
        }
    }

    private void restore(OptionsHelper optionsHelper) throws IOException {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val restorePath = optionsHelper.getOptionValue(OPTION_DIR);

        val restoreMetadataUrl = getMetadataUrl(restorePath, false);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);
        logger.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, restorePath);

        val restoreResourceStore = ResourceStore.getKylinMetaStore(restoreConfig);
        val restoreMetadataStore = restoreResourceStore.getMetadataStore();
        MetadataChecker metadataChecker = new MetadataChecker(restoreMetadataStore);

        val verifyResult = metadataChecker.verify();
        if (!verifyResult.isQualified()) {
            throw new RuntimeException(verifyResult.getResultMessage() + "\n the metadata dir is not qualified");
        }
        restore(resourceStore, restoreResourceStore, project);
        backup(kylinConfig);

    }

    public static void restore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore, String project) {
        if (StringUtils.isBlank(project)) {
            logger.info("start to restore all projects");
            var srcProjectFolders = restoreResourceStore.listResources("/");
            var destProjectFolders = currentResourceStore.listResources("/");
            srcProjectFolders = srcProjectFolders == null ? Sets.newTreeSet() : srcProjectFolders;
            destProjectFolders = destProjectFolders == null ? Sets.newTreeSet() : destProjectFolders;
            val projectFolders = Sets.union(srcProjectFolders, destProjectFolders);

            for (String projectPath : projectFolders) {
                if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)
                        || projectPath.equals(ResourceStore.METASTORE_IMAGE)) {
                    continue;
                }
                val projectName = Paths.get(projectPath).getName(0).toString();
                val destResources = currentResourceStore.listResourcesRecursively(projectPath);
                val srcResources = restoreResourceStore.listResourcesRecursively(projectPath);
                UnitOfWork.doInTransactionWithRetry(
                        () -> doRestore(currentResourceStore, restoreResourceStore, destResources, srcResources),
                        projectName, 1);
            }

        } else {
            logger.info("start to restore project {}", project);
            val destGlobalProjectResources = currentResourceStore.listResourcesRecursively(ResourceStore.PROJECT_ROOT);

            Set<String> globalDestResources = null;
            if (Objects.nonNull(destGlobalProjectResources)) {
                globalDestResources = destGlobalProjectResources.stream()
                        .filter(x -> Paths.get(x).getFileName().toString().equals(String.format("%s.json", project)))
                        .collect(Collectors.toSet());
            }

            val globalSrcResources = restoreResourceStore.listResourcesRecursively(ResourceStore.PROJECT_ROOT).stream()
                    .filter(x -> Paths.get(x).getFileName().toString().equals(String.format("%s.json", project)))
                    .collect(Collectors.toSet());

            Set<String> finalGlobalDestResources = globalDestResources;

            UnitOfWork.doInTransactionWithRetry(() -> doRestore(currentResourceStore, restoreResourceStore,
                    finalGlobalDestResources, globalSrcResources), UnitOfWork.GLOBAL_UNIT, 1);

            val projectPath = "/" + project;
            val destResources = currentResourceStore.listResourcesRecursively(projectPath);
            val srcResources = restoreResourceStore.listResourcesRecursively(projectPath);

            UnitOfWork.doInTransactionWithRetry(
                    () -> doRestore(currentResourceStore, restoreResourceStore, destResources, srcResources), project,
                    1);
        }

        logger.info("restore successfully");
    }

    private static int doRestore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore,
            Set<String> destResources, Set<String> srcResources) throws IOException {
        val threadViewRS = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        //check destResources and srcResources are null,because  Sets.difference(srcResources, destResources) will report NullPointerException
        destResources = destResources == null ? Collections.EMPTY_SET : destResources;
        srcResources = srcResources == null ? Collections.EMPTY_SET : srcResources;

        val insertRes = Sets.difference(srcResources, destResources);
        for (val res : insertRes) {
            val metadataRaw = restoreResourceStore.getResource(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), -1L);
        }

        val updateRes = Sets.intersection(destResources, srcResources);
        for (val res : updateRes) {
            val raw = currentResourceStore.getResource(res);
            val metadataRaw = restoreResourceStore.getResource(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), raw.getMvcc());
        }

        val deleteRes = Sets.difference(destResources, srcResources);
        for (val res : deleteRes) {
            threadViewRS.deleteResource(res);
        }

        return 0;
    }

    String getMetadataUrl(String rootPath, boolean compressed) {
        if (HadoopUtil.isHdfsCompatibleSchema(rootPath, kylinConfig)) {
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