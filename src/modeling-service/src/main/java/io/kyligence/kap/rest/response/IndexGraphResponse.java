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
package io.kyligence.kap.rest.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.Data;

@Data
public class IndexGraphResponse {
    private String project;

    private String model;

    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;

    @JsonProperty("auto_agg_indexes")
    private IndexList autoAggIndexes;

    @JsonProperty("auto_table_indexes")
    private IndexList autoTableIndexes;

    @JsonProperty("manual_agg_indexes")
    private IndexList manualAggIndexes;

    @JsonProperty("manual_table_indexes")
    protected IndexList manualTableIndexes;

    @JsonProperty("is_full_loaded")
    public boolean isFullLoaded() {
        return endTime == Long.MAX_VALUE;
    }

    @Data
    public static class IndexList {

        private List<Index> indexes;

        @JsonProperty("total_size")
        private long totalSize;
    }

    @Data
    public static class Index {

        private long id;

        @JsonProperty("data_size")
        private long dataSize;

        private long usage;

        private IndexEntity.Status status;

        @JsonProperty("last_modified_time")
        private long lastModifiedTime;
    }

    @JsonProperty("empty_indexes")
    private long emptyIndexes;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    @JsonProperty("segment_to_complement_count")
    private long segmentToComplementCount;
}
