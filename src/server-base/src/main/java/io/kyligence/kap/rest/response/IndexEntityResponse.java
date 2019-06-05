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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.CuboidStatus;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Setter
@Getter
public class IndexEntityResponse {

    @JsonProperty("dimensions_res")
    private List<String> dimensionsRes = new ArrayList<>();
    @JsonProperty("measures_res")
    private List<String> measuresRes = new ArrayList<>();

    @JsonProperty("id")
    private long id;
    @JsonProperty("storage_size")
    private long storageSize;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    @JsonProperty("status")
    private CuboidStatus status = CuboidStatus.AVAILABLE;
    @JsonProperty("last_modify_time")
    private long lastModifiedTime;
    @JsonProperty("query_hit_count")
    private long queryHitCount;
    @JsonManagedReference
    @JsonProperty("layouts")
    private List<LayoutEntity> layouts = Lists.newArrayList();

    @JsonBackReference
    private IndexPlan indexPlan;

    public IndexEntityResponse(IndexEntity indexEntity) {
        this.setId(indexEntity.getId());
        this.setIndexPlan(indexEntity.getIndexPlan());
        this.setLayouts(indexEntity.getLayouts());

        setDimensionsAndMeasures(indexEntity);
        val dataflow = NDataflowManager.getInstance(indexPlan.getConfig(), indexPlan.getProject())
                .getDataflow(indexPlan.getUuid());
        Segments<NDataSegment> segments = dataflow.getSegments().getSegmentsExcludeRefreshingAndMerging();
        long storage = 0L;
        long startTime = Long.MAX_VALUE;
        long endTime = 0L;
        if (CollectionUtils.isEmpty(segments)) {
            status = CuboidStatus.EMPTY;
            return;
        }
        for (NDataSegment segment : segments) {
            for (LayoutEntity layout : layouts) {
                NDataLayout dataLayout = segment.getLayout(layout.getId());
                if (dataLayout == null) {
                    status = CuboidStatus.EMPTY;
                    return;
                }
                storage += dataLayout.getByteSize();
            }
            long start = Long.parseLong(segment.getSegRange().getStart().toString());
            long end = Long.parseLong(segment.getSegRange().getEnd().toString());
            startTime = startTime < start ? startTime : start;
            endTime = endTime > end ? endTime : end;
        }
        this.startTime = startTime;
        this.endTime = endTime;
        this.storageSize = storage;

        this.lastModifiedTime = layouts.stream().map(LayoutEntity::getUpdateTime).max(Comparator.naturalOrder())
                .orElse(0L);
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                indexEntity.getIndexPlan().getProject());
        val layoutSet = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        this.queryHitCount = df.getDataflow(indexEntity.getIndexPlan().getId()).getLayoutHitCount().entrySet().stream()
                .filter(entry -> layoutSet.contains(entry.getKey())).map(Map.Entry::getValue)
                .flatMap(hit -> hit.getDateFrequency().values().stream()).mapToInt(Integer::intValue).sum();

    }

    private void setDimensionsAndMeasures(IndexEntity indexEntity) {
        ImmutableSet<TblColRef> dimensionSet = indexEntity.getDimensionSet();
        if (!CollectionUtils.isEmpty(dimensionSet)) {
            for (TblColRef dimension : dimensionSet) {
                this.dimensionsRes.add(dimension.getName());
            }
        }

        ImmutableSet<NDataModel.Measure> measureSet = indexEntity.getMeasureSet();
        if (!CollectionUtils.isEmpty(measureSet)) {
            for (NDataModel.Measure measure : measureSet) {
                this.measuresRes.add(measure.getName());
            }
        }
    }
}