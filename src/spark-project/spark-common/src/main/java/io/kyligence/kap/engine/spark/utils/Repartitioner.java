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

package io.kyligence.kap.engine.spark.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class Repartitioner {
    protected static final Logger logger = LoggerFactory.getLogger(Repartitioner.class);

    private int MB = 1024 * 1024;
    private int shardSize;
    private int fileLengthThreshold;
    private long totalRowCount;
    private long rowCountThreshold;
    private ContentSummary contentSummary;
    private List<Integer> shardByColumns = new ArrayList<>();
    private List<Integer> sortByColumns;

    public Repartitioner(int shardSize, int fileLengthThreshold, long totalRowCount, long rowCountThreshold,
            ContentSummary contentSummary, List<Integer> shardByColumns, List<Integer> sortByColumns) {
        this.shardSize = shardSize;
        this.fileLengthThreshold = fileLengthThreshold;
        this.totalRowCount = totalRowCount;
        this.rowCountThreshold = rowCountThreshold;
        this.contentSummary = contentSummary;
        if (shardByColumns != null) {
            this.shardByColumns = shardByColumns;
        }
        this.sortByColumns = sortByColumns;
    }

    boolean needRepartitionForFileSize() {
        // per file size < threshold file size
        return (contentSummary.getLength() * 1.0 / MB / contentSummary.getFileCount()) < fileLengthThreshold
                && contentSummary.getFileCount() > 1;
    }

    boolean needRepartitionForShardByColumns() {
        return shardByColumns != null && !shardByColumns.isEmpty();
    }

    private boolean needRepartitionForRowCount() {
        return contentSummary.getFileCount() < ((double) totalRowCount / rowCountThreshold) * 0.75;
    }

    @VisibleForTesting
    public boolean needRepartition() {
        if (needRepartitionForShardByColumns()) {
            return true;
        }
        boolean needRepartition = needRepartitionForFileSize() || needRepartitionForRowCount();

        if (needRepartition && getRepartitionNumByStorage() == contentSummary.getFileCount()) {
            needRepartition = false;
        }
        return needRepartition;
    }

    public int getShardSize() {
        return shardSize;
    }

    public int getFileLengthThreshold() {
        return fileLengthThreshold;
    }

    public ContentSummary getContentSummary() {
        return contentSummary;
    }

    private List<Integer> getShardByColumns() {
        return shardByColumns;
    }

    private int getFileLengthRepartitionNum() {
        return (int) Math.ceil(contentSummary.getLength() * 1.0 / MB / shardSize);
    }

    private int getRowCountRepartitionNum() {
        return (int) Math.ceil(1.0 * totalRowCount / rowCountThreshold);
    }

    public int getRepartitionNumByStorage() {
        int fileLengthRepartitionNum = getFileLengthRepartitionNum();
        int rowCountRepartitionNum = getRowCountRepartitionNum();
        logger.info("File length repartition num : {}, Row count Rpartition num: {}", fileLengthRepartitionNum,
                rowCountRepartitionNum);
        int partitionSize = (int) Math.ceil(1.0 * (fileLengthRepartitionNum + rowCountRepartitionNum) / 2);
        logger.info("Repartition size is :{}", partitionSize);
        return partitionSize;
    }

    public void doRepartition(String outputPath, String inputPath, int repartitionNum, SparkSession ss) throws IOException {
        Path tempResourcePath = new Path(inputPath);

        FileSystem readFileSystem = HadoopUtil.getWorkingFileSystem();
        if (needRepartition()) {
            // repartition and write to target path
            logger.info("Repartition {} to {}, [repartition number: {}, use shard column: {}]",
                    inputPath, outputPath, repartitionNum, needRepartitionForShardByColumns());
            long start = System.currentTimeMillis();
            Dataset<Row> data;

            if (needRepartitionForShardByColumns()) {
                ss.sessionState().conf().setLocalProperty("spark.sql.adaptive.enabled", "false");
                data = ss.read().parquet(inputPath)
                        .repartition(repartitionNum, convertIntegerToColumns(getShardByColumns()))
                        .sortWithinPartitions(convertIntegerToColumns(sortByColumns));
            } else {
                // repartition for single file size is too small
                data = ss.read().parquet(inputPath).repartition(repartitionNum)
                        .sortWithinPartitions(convertIntegerToColumns(sortByColumns));
            }

            data.write().mode(SaveMode.Overwrite).parquet(outputPath);
            if (needRepartitionForShardByColumns()) {
                ss.sessionState().conf().setLocalProperty("spark.sql.adaptive.enabled", null);
            }
            if (readFileSystem.delete(tempResourcePath, true)) {
                logger.info("Delete temp cuboid path successful. Temp path: {}.", inputPath);
            } else {
                logger.error("Delete temp cuboid path wrong, leave garbage. Temp path: {}.", inputPath);
            }
            long end = System.currentTimeMillis();
            logger.info("Repartition and rewrite ends. Cost: {} ms.", end - start);
        } else {
            Path goalPath = new Path(outputPath);
            if (readFileSystem.exists(goalPath)) {
                logger.info("Path {} is exists, delete it.", goalPath);
                readFileSystem.delete(goalPath, true);
            }
            if (readFileSystem.rename(new Path(inputPath), goalPath)) {
                logger.info("Rename temp path to target path successfully. Temp path: {}, target path: {}.", inputPath,
                        outputPath);
            } else {
                throw new RuntimeException(String.format(
                        "Rename temp path to target path wrong. Temp path: %s, target path: %s.", inputPath, outputPath));
            }
        }
    }

    private Column[] convertIntegerToColumns(List<Integer> indices) {
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }
}
