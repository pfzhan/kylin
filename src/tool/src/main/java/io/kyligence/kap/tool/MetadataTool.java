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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.var;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.cluster.NodeCandidate;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataTool extends AbstractApplication {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private static final String HDFS_METADATA_URL_FROMATTER = "kylin_metadata@hdfs,path=%s";

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
    private final LeaderInitiator leaderInitiator;

    private ResourceStore resourceStore;

    MetadataTool() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        leaderInitiator = LeaderInitiator.getInstance(kylinConfig);
        this.options = new Options();
        initOptions();
    }

    public MetadataTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        leaderInitiator = LeaderInitiator.getInstance(kylinConfig);
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
        try {
            tool.execute(args);
            System.exit(0);
        } catch (Exception e) {
            log.error("", e);
            System.exit(1);
        }
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        val candidate = new NodeCandidate(kylinConfig.getNodeId());
        leaderInitiator.start(candidate);

        if (!leaderInitiator.isLeader()) {
            throw new IllegalStateException("The leader is not avalilable");
        }

        log.info("start to init ResourceStore");
        resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            backup(optionsHelper);
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
            folder = LocalDateTime.now().format(DATE_TIME_FORMATTER)
                    + "_backup";
        }
        val backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        val backupMetadataUrl = getMetadataUrl(backupPath);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        log.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);

        val backupMetadataStore = ResourceStore.createMetadataStore(backupConfig, MetadataStore.METADATA_NAMESPACE);

        if (StringUtils.isBlank(project)) {
            log.info("start to backup all projects");
            backupMetadataStore.dump(resourceStore);
        } else {
            log.info("start to backup project {}", project);
            backupMetadataStore.dump(resourceStore, "/" + project);
            backupMetadataStore.putResource(resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG));
        }

        log.info("backup successfully");
    }


    private void restore(OptionsHelper optionsHelper) throws IOException {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val restorePath = optionsHelper.getOptionValue(OPTION_DIR);

        val restoreMetadataUrl = getMetadataUrl(restorePath);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);
        log.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, restorePath);

        val restoreMetadataStore = ResourceStore.createMetadataStore(restoreConfig, MetadataStore.METADATA_NAMESPACE);

        val verifyResult = restoreMetadataStore.verify();
        if (!verifyResult.isQualified()) {
            throw new RuntimeException(verifyResult.getResultMessage() + "\n the metadata dir is not qualified");
        }

        if (StringUtils.isBlank(project)) {
            log.info("start to restore all projects");
            val projectFolders = resourceStore.listResources("/");
            for (String projectPath : projectFolders) {
                if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)) {
                    continue;
                }
                val projectName = projectPath.substring(1);
                val distResources = resourceStore.listResourcesRecursively(projectPath);
                val srcResources = restoreMetadataStore.list(projectPath).stream().map(x -> projectPath + x)
                        .collect(Collectors.toSet());
                UnitOfWork.doInTransactionWithRetry(() -> doRestore(restoreMetadataStore, distResources, srcResources),
                        projectName);
            }

        } else {
            log.info("start to restore project {}", project);
            val projectPath = "/" + project;
            val distResources = resourceStore.listResourcesRecursively(projectPath);
            val srcResources = restoreMetadataStore.list(projectPath).stream().map(x -> projectPath + x)
                    .collect(Collectors.toSet());
            UnitOfWork.doInTransactionWithRetry(() -> doRestore(restoreMetadataStore, distResources, srcResources),
                    project);
        }

        log.info("restore successfully");
    }

    private int doRestore(MetadataStore restoreMetadataStore, Set<String> distResources, Set<String> srcResources)
            throws IOException {
        val threadViewRS = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        //check distResources and srcResources are null,because  Sets.difference(srcResources, distResources) will report NullPointerException
        distResources = distResources == null ? Collections.EMPTY_SET : distResources;
        srcResources = srcResources == null ? Collections.EMPTY_SET : srcResources;

        val insertRes = Sets.difference(srcResources, distResources);
        for (val res : insertRes) {
            val metadataRaw = restoreMetadataStore.load(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), -1L);
        }

        val updateRes = Sets.intersection(distResources, srcResources);
        for (val res : updateRes) {
            val raw = resourceStore.getResource(res);
            val metadataRaw = restoreMetadataStore.load(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), raw.getMvcc());
        }

        val deleteRes = Sets.difference(distResources, srcResources);
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