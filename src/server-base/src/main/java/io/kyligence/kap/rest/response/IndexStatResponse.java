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

import static io.kyligence.kap.metadata.cube.model.IndexEntity.Status.LOCKED;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
public class IndexStatResponse {

    @JsonProperty("max_data_size")
    private long maxDataSize;

    @JsonProperty("max_usage")
    private long maxUsage;

    @JsonProperty("has_load_base_table_index")
    private boolean hasLoadBaseTableIndex;

    @JsonProperty("has_load_base_agg_index")
    private boolean hasLoadBaseAggIndex;

    public static IndexStatResponse from(List<IndexResponse> results) {
        IndexStatResponse response = new IndexStatResponse();
        long maxUsage = 0;
        long maxDataSize = 0;
        for (IndexResponse index : results) {
            if (index.isBase() && index.getStatus() != LOCKED && IndexEntity.isAggIndex(index.getId())) {
                response.setHasLoadBaseAggIndex(true);
            }
            if (index.isBase() && index.getStatus() != LOCKED && IndexEntity.isTableIndex(index.getId())) {
                response.setHasLoadBaseTableIndex(true);
            }
            maxDataSize = Math.max(maxDataSize, index.getDataSize());
            maxUsage = Math.max(maxUsage, index.getUsage());
        }
        response.setMaxDataSize(maxDataSize);
        response.setMaxUsage(maxUsage);
        return response;
    }
}
