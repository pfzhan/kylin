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
package org.apache.kylin.rest.response;

import static io.kyligence.kap.metadata.cube.model.IndexEntity.Range.BATCH;
import static io.kyligence.kap.metadata.cube.model.IndexEntity.Range.HYBRID;
import static io.kyligence.kap.metadata.cube.model.IndexEntity.Range.STREAMING;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;


@Getter
@AllArgsConstructor
public class AggIndexResponse implements Serializable {

    private static AggIndexResponse EMPTY = new AggIndexResponse(Lists.newArrayList(),
            AggIndexCombResult.successResult(0), 0L);

    @JsonProperty(value = "agg_index_counts")
    private List<AggIndexCombResult> aggIndexCounts;
    @JsonProperty(value = "total_count")
    private AggIndexCombResult totalCount;
    @JsonProperty(value = "max_combination_num")
    private Long aggrgroupMaxCombination;

    public static AggIndexResponse combine(AggIndexResponse batch, AggIndexResponse stream,
            List<IndexEntity.Range> aggGroupTypes) {
        if (batch.isEmpty()) {
            return stream;
        }
        if (stream.isEmpty()) {
            return batch;
        }
        val combineTotalCount = AggIndexCombResult.combine(batch.getTotalCount(), stream.getTotalCount());
        val aggIndexCounts = Lists.<AggIndexCombResult> newArrayList();

        int batchIndex = 0;
        int streamIndex = 0;
        for (int n = 0; n < aggGroupTypes.size(); n++) {
            if (aggGroupTypes.get(n) == BATCH) {
                aggIndexCounts.add(batch.getAggIndexCounts().get(batchIndex++));
            } else if (aggGroupTypes.get(n) == STREAMING) {
                aggIndexCounts.add(stream.getAggIndexCounts().get(streamIndex++));
            } else if (aggGroupTypes.get(n) == HYBRID){
                aggIndexCounts.add(AggIndexCombResult.combine(batch.getAggIndexCounts().get(batchIndex++),
                        stream.getAggIndexCounts().get(streamIndex++)));
            }

        }
        AggIndexResponse combineResponse = new AggIndexResponse(aggIndexCounts, combineTotalCount,
                stream.getAggrgroupMaxCombination());
        return combineResponse;
    }

    private boolean isEmpty() {
        return EMPTY == this;
    }

    public static AggIndexResponse empty() {
        return EMPTY;
    }
}
