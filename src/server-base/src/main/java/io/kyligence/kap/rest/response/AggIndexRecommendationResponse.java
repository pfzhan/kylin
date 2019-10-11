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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
@NoArgsConstructor
public class AggIndexRecommendationResponse extends AggIndexRecomDetailResponse {
    @JsonProperty("item_id")
    private long itemId;

    // there will have multiple item ids when recommendation type is modification
    @JsonProperty("item_ids")
    private List<Long> itemIds;

    @JsonProperty("storage_size")
    private long storageSize;

    @JsonProperty("query_hit_count")
    private long queryHitCount;

    @JsonProperty("recommendation_type")
    private RecommendationType recommendationType;

    public AggIndexRecommendationResponse(IndexEntity indexEntity, NDataModel optimizedModel) {
        super(indexEntity, optimizedModel, null, OptRecommendationResponse.PAGING_OFFSET, OptRecommendationResponse.PAGING_SIZE);

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), optimizedModel.getProject())
                .getDataflow(optimizedModel.getId());
        if (dataflow == null)
            return;

        val layoutSet = indexEntity.getLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        this.queryHitCount = dataflow.getLayoutHitCount().entrySet().stream()
                .filter(entry -> layoutSet.contains(entry.getKey())).map(Map.Entry::getValue)
                .flatMap(hit -> hit.getDateFrequency().values().stream()).mapToInt(Integer::intValue).sum();

        Segments<NDataSegment> segments = dataflow.getSegments().getSegmentsExcludeRefreshingAndMerging();

        for (NDataSegment segment : segments) {
            if (segment.getStatus().equals(SegmentStatusEnum.NEW)) {
                continue;
            }
            for (LayoutEntity layout : indexEntity.getLayouts()) {
                NDataLayout dataLayout = segment.getLayout(layout.getId());
                if (dataLayout == null) {
                    return;
                }
                this.storageSize += dataLayout.getByteSize();
            }
        }
    }

    public void setItemIds(List<Long> itemIds) {
        this.itemIds = itemIds;
        this.itemId = itemIds.stream().mapToLong(item -> item).max().orElseThrow(NoSuchElementException::new);
    }
}
