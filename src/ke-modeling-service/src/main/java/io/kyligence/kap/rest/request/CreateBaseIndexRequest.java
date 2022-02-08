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
package io.kyligence.kap.rest.request;

import java.util.List;
import java.util.Set;


import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.IndexEntity.Source;
import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class CreateBaseIndexRequest implements ProjectInsensitiveRequest {

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("source_types")
    private Set<Source> sourceTypes = Sets.newHashSet();

    @JsonProperty("base_agg_index_property")
    private LayoutProperty baseAggIndexProperty;

    @JsonProperty("base_table_index_property")
    private LayoutProperty baseTableIndexProperty;

    public boolean needHandleBaseAggIndex() {
        return sourceTypes.isEmpty() || sourceTypes.contains(Source.BASE_AGG_INDEX);
    }

    public boolean needHandleBaseTableIndex() {
        return sourceTypes.isEmpty() || sourceTypes.contains(Source.BASE_TABLE_INDEX);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LayoutProperty {

        @JsonProperty("col_order")
        private List<String> colOrder;

        @JsonProperty("shard_by_columns")
        private List<String> shardByColumns;

        @JsonProperty("sort_by_columns")
        private List<String> sortByColumns;
    }

}
