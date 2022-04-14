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

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class OpenGetIndexResponse implements Serializable {

    @JsonProperty("project")
    private String project;
    @JsonProperty("uuid")
    private String modelId;
    @JsonProperty("model_name")
    private String modelAlias;
    @JsonProperty("total_size")
    private int totalSize;
    @JsonProperty("offset")
    private int offset;
    @JsonProperty("limit")
    private int limit;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("indexes")
    private List<IndexDetail> indexDetailList;
    @JsonProperty("absent_batch_index_ids")
    private List<Long> absentBatchIndexIds;

    @Getter
    @Setter
    @NoArgsConstructor
    public static class IndexDetail {

        @JsonProperty("id")
        private long id;
        @JsonProperty("status")
        private IndexEntity.Status status;
        @JsonProperty("source")
        private IndexEntity.Source source;
        @JsonProperty("col_order")
        private List<IndexResponse.ColOrderPair> colOrder;
        @JsonProperty("shard_by_columns")
        private List<String> shardByColumns;
        @JsonProperty("sort_by_columns")
        private List<String> sortByColumns;
        @JsonProperty("data_size")
        private long dataSize;
        @JsonProperty("usage")
        private long usage;
        @JsonProperty("last_modified")
        private long lastModified;
        @JsonProperty("storage_type")
        private int storageType;
        @JsonProperty("related_tables")
        private List<String> relatedTables;

        public static IndexDetail newIndexDetail(IndexResponse indexResponse) {
            IndexDetail detail = new IndexDetail();
            detail.setId(indexResponse.getId());
            detail.setStatus(indexResponse.getStatus());
            detail.setSource(indexResponse.getSource());
            detail.setColOrder(indexResponse.getColOrder());
            detail.setShardByColumns(indexResponse.getShardByColumns());
            detail.setSortByColumns(indexResponse.getSortByColumns());
            detail.setDataSize(indexResponse.getDataSize());
            detail.setUsage(indexResponse.getUsage());
            detail.setLastModified(indexResponse.getLastModified());
            detail.setStorageType(indexResponse.getStorageType());
            detail.setRelatedTables(indexResponse.getRelatedTables());
            return detail;
        }
    }

}
