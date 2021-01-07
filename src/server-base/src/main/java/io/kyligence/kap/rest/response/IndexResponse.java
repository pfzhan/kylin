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

import org.apache.kylin.metadata.model.IStorageAware;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class IndexResponse {
    private Long id;

    private String project;

    private String model;

    private String name;

    private String owner;

    private IndexEntity.Status status;

    @JsonProperty("col_order")
    private List<ColOrderPair> colOrder;

    @JsonProperty("shard_by_columns")
    private List<String> shardByColumns;

    @JsonProperty("sort_by_columns")
    private List<String> sortByColumns;

    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_NDATA_STORAGE;

    @JsonProperty("data_size")
    private long dataSize;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("last_modified")
    private long lastModified;

    @JsonIgnore
    private boolean isManual;

    @JsonProperty("source")
    public IndexEntity.Source getSource() {
        if (getId() < IndexEntity.TABLE_INDEX_START_ID) {
            if (isManual()) {
                return IndexEntity.Source.CUSTOM_AGG_INDEX;
            } else {
                return IndexEntity.Source.RECOMMENDED_AGG_INDEX;
            }
        } else {
            if (isManual()) {
                return IndexEntity.Source.CUSTOM_TABLE_INDEX;
            } else {
                return IndexEntity.Source.RECOMMENDED_TABLE_INDEX;
            }
        }

    }

    @Data
    @AllArgsConstructor
    public static class ColOrderPair {
        private String key;
        private String value;
        private Long cardinality;

        public ColOrderPair(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
