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

package io.kyligence.kap.metadata.cube.model;

public interface NBatchConstants {
    String P_DATAFLOW_ID = "dataflowId";
    String P_SEGMENT_IDS = "segmentIds";
    String P_JOB_ID = "jobId";
    String P_JOB_TYPE = "jobType";
    String P_LAYOUT_IDS = "layoutIds";
    String P_TO_BE_DELETED_LAYOUT_IDS = "toBeDeletedLayoutIds";
    String P_CLASS_NAME = "className";
    String P_JARS = "jars";
    String P_DIST_META_URL = "distMetaUrl";
    String P_OUTPUT_META_URL = "outputMetaUrl";
    String P_PROJECT_NAME = "project";
    String P_TABLE_NAME = "table";
    String P_SAMPLING_ROWS = "samplingRows";
    String P_TARGET_MODEL = "targetModel";
    String P_DATA_RANGE_START = "dataRangeStart";
    String P_DATA_RANGE_END = "dataRangeEnd";
    String P_EXCLUDED_TABLES = "excludedTables";

    String P_IGNORED_SNAPSHOT_TABLES = "ignoredSnapshotTables";
    String P_NEED_BUILD_SNAPSHOTS = "needBuildSnapshots";
    String P_PARTITION_IDS = "partitionIds";
    String P_BUCKETS = "buckets";

    String P_INCREMENTAL_BUILD = "incrementalBuild";
    String P_SELECTED_PARTITION_COL = "selectedPartitionCol";
    String P_SELECTED_PARTITION_VALUE = "selectedPartition";
    String P_PARTIAL_BUILD = "partialBuild";

    String P_QUERY_ID = "queryId";
    String P_QUERY_PARAMS = "queryParams";
    String P_QUERY_CONTEXT = "queryContext";
    String P_QUERY_QUEUE = "queryQueue";

    /** use for stage calculate exec ratio */
    String P_INDEX_COUNT = "indexCount";
    String P_INDEX_SUCCESS_COUNT = "indexSuccessCount";
    /** value like : { "segmentId1": 1223, "segmentId2": 1223 } */
    String P_WAITE_TIME = "waiteTime";

    // ut only
    String P_BREAK_POINT_LAYOUTS = "breakPointLayouts";
}
