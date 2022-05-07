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

package io.kyligence.kap.streaming.constants;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;

public class StreamingConstants {

    // spark job conf
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_MASTER_DEFAULT = "yarn";
    public static final String SPARK_DRIVER_MEM = "spark.driver.memory";
    public static final String SPARK_DRIVER_MEM_DEFAULT = "512m";
    public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String SPARK_EXECUTOR_INSTANCES_DEFAULT = "2";
    public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
    public static final String SPARK_CORES_MAX = "spark.cores.max";
    public static final String SPARK_EXECUTOR_CORES_DEFAULT = "2";
    public static final String SPARK_EXECUTOR_MEM = "spark.executor.memory";
    public static final String SPARK_EXECUTOR_MEM_DEFAULT = "1g";
    public static final String SPARK_DRIVER_OVERHEAD = "spark.driver.memoryOverhead";
    public static final String SPARK_DRIVER_OVERHEAD_DEFAULT = "1g";

    public static final String SPARK_YARN_DIST_JARS = "spark.yarn.dist.jars";
    public static final String SPARK_DRIVER_OPTS = "spark.driver.extraJavaOptions";
    public static final String SPARK_EXECUTOR_OPTS = "spark.executor.extraJavaOptions";
    public static final String SPARK_YARN_AM_OPTS = "spark.yarn.am.extraJavaOptions";
    public static final String SPARK_YARN_TIMELINE_SERVICE = "spark.hadoop.yarn.timeline-service.enabled";
    public static final String SPARK_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    public static final String SPARK_SHUFFLE_PARTITIONS_DEFAULT = "8";

    //rest server
    public static final String REST_SERVER_IP = "kylin.spark.rest.server.ip";

    // main class
    public static final String SPARK_STREAMING_ENTRY = "io.kyligence.kap.streaming.app.StreamingEntry";
    public static final String SPARK_STREAMING_MERGE_ENTRY = "io.kyligence.kap.streaming.app.StreamingMergeEntry";

    // default parser
    public static final String DEFAULT_PARSER_NAME = "io.kyligence.kap.parser.TimedJsonStreamParser";

    // hadoop conf
    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    public static final String SLASH = "/";

    // streaming job
    public static final String STREAMING_META_URL = "kylin.streaming.meta-scheme";
    public static final String STREAMING_META_URL_DEFAULT = HDFSMetadataStore.HDFS_SCHEME;
    public static final String STREAMING_DURATION = "kylin.streaming.duration";
    public static final String STREAMING_CONFIG_PREFIX = "kylin.streaming.spark-conf.";
    public static final String STREAMING_DURATION_DEFAULT = "30";
    public static final String FILE_LAYER = "file_layer";
    public static final String ACTION_START = "START";
    public static final String ACTION_GRACEFUL_SHUTDOWN = "GRACEFUL_SHUTDOWN";

    // watermark
    public static final String STREAMING_WATERMARK = "kylin.streaming.watermark";
    public static final String STREAMING_WATERMARK_DEFAULT = KylinConfig.getInstanceFromEnv()
            .getStreamingJobWatermark();

    // kafka conf
    public static final String STREAMING_KAFKA_CONFIG_PREFIX = "kylin.streaming.kafka-conf.";
    public static final String STREAMING_MAX_OFFSETS_PER_TRIGGER = "kylin.streaming.kafka-conf.maxOffsetsPerTrigger";
    public static final String STREAMING_MAX_OFFSETS_PER_TRIGGER_DEFAULT = KylinConfig.getInstanceFromEnv()
            .getKafkaMaxOffsetsPerTrigger();
    public static final String STREAMING_KAFKA_STARTING_OFFSETS = "kylin.streaming.kafka-conf.startingOffsets";

    // merge job
    public static final String STREAMING_SEGMENT_MAX_SIZE = "kylin.streaming.segment-max-size";
    public static final String STREAMING_SEGMENT_MAX_SIZE_DEFAULT = "32m";
    public static final String STREAMING_SEGMENT_MERGE_THRESHOLD = "kylin.streaming.segment-merge-threshold";
    public static final String STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT = "3";

    // retry
    public static final String STREAMING_RETRY_ENABLE = "kylin.streaming.job-retry-enabled";

    // dimension table refresh conf
    public static final String STREAMING_TABLE_REFRESH_INTERVAL = "kylin.streaming.table-refresh-interval";

}
