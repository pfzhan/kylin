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

package io.kyligence.kap.engine.spark.job;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.SnapshotBuilder;
import io.kyligence.kap.engine.spark.builder.SnapshotPartitionBuilder;
import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.SparkConfHelper;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class SnapshotBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(SnapshotBuildJob.class);

    @Override
    protected void doExecute() throws Exception {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String selectedPartCol = getParam(NBatchConstants.P_SELECTED_PARTITION_COL);
        TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        boolean incrementalBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));

        if (selectedPartCol == null) {
            new SnapshotBuilder().buildSnapshot(ss, Sets.newHashSet(tableDesc));
        } else {
            initialize(tableDesc, selectedPartCol, incrementalBuild);

            tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            logger.info("{} need build partitions: {}", tableDesc.getIdentity(), tableDesc.getNotReadyPartitions());

            new SnapshotPartitionBuilder().buildSnapshot(ss, tableDesc, selectedPartCol);

            if (incrementalBuild) {
                moveIncrementalPartitions(tableDesc.getLastSnapshotPath(), tableDesc.getTempSnapshotPath());
            }
        }
    }

    private void initialize(TableDesc table, String selectedPartCol, boolean incrementBuild) {
        if (table.getTempSnapshotPath() != null) {
            logger.info("snapshot partition has been initialed, so skip.");
            return;
        }
        Set<String> partitions = getTablePartitions(table, selectedPartCol);
        Set<String> curPartitions = table.getSnapshotPartitions().keySet();
        String resourcePath = FileNames.snapshotFile(table) + "/" + UUID.randomUUID();

        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tableMetadataManager = NTableMetadataManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project);
            TableDesc copy = tableMetadataManager.copyForWrite(table);
            if (incrementBuild) {
                copy.addSnapshotPartitions(Sets.difference(partitions, curPartitions));
            } else {
                copy.resetSnapshotPartitions(partitions);
                TableExtDesc copyExt = tableMetadataManager
                        .copyForWrite(tableMetadataManager.getOrCreateTableExt(table));
                copyExt.setTotalRows(0);
                tableMetadataManager.saveTableExt(copyExt);
            }
            copy.setTempSnapshotPath(resourcePath);
            tableMetadataManager.updateTableDesc(copy);
            return null;
        }, project);

    }

    private Set<String> getTablePartitions(TableDesc tableDesc, String selectPartitionCol) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return toPartitions(getParam("partitions"));
        }
        ISourceMetadataExplorer explr = SourceFactory.getSource(tableDesc).getSourceMetadataExplorer();
        Set<String> curPartitions = explr.getTablePartitions(tableDesc.getDatabase(), tableDesc.getName(),
                tableDesc.getProject(), selectPartitionCol);

        logger.info("{} current partitions: {}", tableDesc.getIdentity(), curPartitions);
        return curPartitions;
    }

    private static Set<String> toPartitions(String tableListStr) {
        if (StringUtils.isBlank(tableListStr)) {
            return null;
        }
        return ImmutableSet.<String> builder().addAll(Arrays.asList(StringSplitter.split(tableListStr, ","))).build();
    }

    private void moveIncrementalPartitions(String originSnapshotPath, String incrementalSnapshotPath) {
        String target = getSnapshotDir(originSnapshotPath);
        Path sourcePath = new Path(getSnapshotDir(incrementalSnapshotPath));
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            if (!fs.exists(sourcePath)) {
                return;
            }
            for (FileStatus fileStatus : fs.listStatus(sourcePath)) {
                Path targetFilePath = new Path(target + "/" + fileStatus.getPath().getName());
                if (fs.exists(targetFilePath)) {
                    logger.info(String.format(Locale.ROOT, "delete non-effective partition %s ", targetFilePath));
                    fs.delete(targetFilePath, true);
                }
                fs.rename(fileStatus.getPath(), new Path(target));
            }

            fs.delete(sourcePath, false);
        } catch (Exception e) {
            logger.error(String.format(Locale.ROOT, "from %s to %s move file fail:", incrementalSnapshotPath,
                    originSnapshotPath), e);
            Throwables.propagate(e);
        }

    }

    private String getSnapshotDir(String snapshotPath) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
        return workingDir + "/" + snapshotPath;
    }

    @Override
    protected void chooseContentSize(SparkConfHelper helper) {
        return;
    }

    public static void main(String[] args) {
        SnapshotBuildJob snapshotBuildJob = new SnapshotBuildJob();
        snapshotBuildJob.execute(args);
    }

}
