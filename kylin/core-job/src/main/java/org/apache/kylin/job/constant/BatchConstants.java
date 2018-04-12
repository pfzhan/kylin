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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.


 */

package org.apache.kylin.job.constant;

public interface BatchConstants {

    /**
     * source data config
     */
    char INTERMEDIATE_TABLE_ROW_DELIMITER = 127;

    /**
     * ConFiGuration entry names for MR jobs
     */

    String CFG_CUBE_NAME = "cube.name";
    String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";
    String CFG_CUBE_SEGMENT_ID = "cube.segment.id";
    String CFG_CUBE_CUBOID_LEVEL = "cube.cuboid.level";

    String CFG_II_NAME = "ii.name";
    String CFG_II_SEGMENT_NAME = "ii.segment.name";

    String CFG_OUTPUT_PATH = "output.path";
    String CFG_PROJECT_NAME = "project.name";
    String CFG_TABLE_NAME = "table.name";
    String CFG_IS_MERGE = "is.merge";
    String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";
    String CFG_REGION_NUMBER_MIN = "region.number.min";
    String CFG_REGION_NUMBER_MAX = "region.number.max";
    String CFG_REGION_SPLIT_SIZE = "region.split.size";
    String CFG_HFILE_SIZE_GB = "hfile.size.gb";
    String CFG_STATS_JOB_ID = "stats.job.id";
    String CFG_STATS_JOB_FREQUENCY = "stats.sample.frequency";

    String CFG_KYLIN_LOCAL_TEMP_DIR = "/tmp/kylin/";
    String CFG_KYLIN_HDFS_TEMP_DIR = "/tmp/kylin/";

    String CFG_STATISTICS_LOCAL_DIR = CFG_KYLIN_LOCAL_TEMP_DIR + "cuboidstatistics/";
    String CFG_STATISTICS_ENABLED = "statistics.enabled";
    String CFG_STATISTICS_OUTPUT = "statistics.ouput";//spell error, for compatibility issue better not change it
    String CFG_STATISTICS_SAMPLING_PERCENT = "statistics.sampling.percent";
    String CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME = "cuboid_statistics.seq";

    String CFG_MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";

    String CFG_OUTPUT_COLUMN = "column";
    String CFG_OUTPUT_DICT = "dict";
    String CFG_OUTPUT_STATISTICS = "statistics";
    String CFG_OUTPUT_PARTITION = "partition";
    String CFG_MR_SPARK_JOB = "mr.spark.job";
    String CFG_SPARK_META_URL = "spark.meta.url";

    /**
     * command line ARGuments
     */
    String ARG_INPUT = "input";
    String ARG_OUTPUT = "output";
    String ARG_PROJECT = "project";
    String ARG_JOB_NAME = "jobname";
    String ARG_CUBING_JOB_ID = "cubingJobId";
    String ARG_CUBE_NAME = "cubename";
    String ARG_II_NAME = "iiname";
    String ARG_SEGMENT_NAME = "segmentname";
    String ARG_SEGMENT_ID = "segmentid";
    String ARG_PARTITION = "partitions";
    String ARG_STATS_ENABLED = "statisticsenabled";
    String ARG_STATS_OUTPUT = "statisticsoutput";
    String ARG_STATS_SAMPLING_PERCENT = "statisticssamplingpercent";
    String ARG_HTABLE_NAME = "htablename";
    String ARG_INPUT_FORMAT = "inputformat";
    String ARG_LEVEL = "level";
    String ARG_CONF = "conf";

    /**
     * logger and counter
     */
    String MAPREDUCE_COUNTER_GROUP_NAME = "Cube Builder";
    int NORMAL_RECORD_LOG_THRESHOLD = 100000;

    /**
     * dictionaries builder class
     */
    String GLOBAL_DICTIONNARY_CLASS = "org.apache.kylin.dict.GlobalDictionaryBuilder";





    String KYLIN_CUBE_ID = "io.kylin.job.cubeid";
    String KYLIN_SEGMENT_ID = "io.kylin.job.segmentid";
    String KYLIN_COLUMNAR_DFS_REPLICATION = "io.kylin.storage.columnar.dfs-replication";
    String KYLIN_CUBOID_LAYOUT_ID = "io.kylin.job.cuboid.layout_id";
}
