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

import static io.kyligence.kap.metadata.model.NDataModel.MEASURE_ID_BASE;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BuildBaseIndexResponse extends BasicResponse {

    public static final BuildBaseIndexResponse EMPTY = new BuildBaseIndexResponse();

    @JsonProperty("base_table_index")
    private IndexInfo tableIndex;

    @JsonProperty("base_agg_index")
    private IndexInfo aggIndex;

    @JsonIgnore
    private boolean isCleanSecondStorage = false;

    public static BuildBaseIndexResponse from(IndexPlan indexPlan) {
        BuildBaseIndexResponse response = new BuildBaseIndexResponse();
        response.addLayout(indexPlan.getBaseAggLayout());
        response.addLayout(indexPlan.getBaseTableLayout());
        return response;
    }

    public void addLayout(LayoutEntity baseLayout) {
        if (baseLayout == null) {
            return;
        }
        IndexInfo info = new IndexInfo();

        info.setDimCount((int) baseLayout.getColOrder().stream().filter(id -> id < MEASURE_ID_BASE).count());
        info.setMeasureCount((int) baseLayout.getColOrder().stream().filter(id -> id >= MEASURE_ID_BASE).count());

        if (IndexEntity.isTableIndex(baseLayout.getId())) {
            tableIndex = info;
        } else {
            aggIndex = info;
        }
        info.setLayoutId(baseLayout.getId());
    }

    public void judgeIndexOperateType(boolean previousExist, boolean isAgg) {
        IndexInfo index = isAgg ? aggIndex : tableIndex;
        if (index != null) {
            if (previousExist) {
                index.setOperateType(OperateType.UPDATE);
            } else {
                index.setOperateType(OperateType.CREATE);
            }
        }
    }

    public void setIndexUpdateType(Set<Long> ids) {
        for (long id : ids) {
            if (IndexEntity.isAggIndex(id) && aggIndex != null) {
                aggIndex.setOperateType(OperateType.UPDATE);
            }
            if (IndexEntity.isTableIndex(id) && tableIndex != null) {
                tableIndex.setOperateType(OperateType.UPDATE);
            }
        }
    }

    public boolean hasIndexChange() {
        return tableIndex != null || aggIndex != null;
    }

    public boolean hasTableIndexChange() {
        return tableIndex != null;
    }

    @Data
    private static class IndexInfo {

        @JsonProperty("dimension_count")
        private int dimCount;

        @JsonProperty("measure_count")
        private int measureCount;

        @JsonProperty("layout_id")
        private long layoutId;

        @JsonProperty("operate_type")
        private OperateType operateType = OperateType.CREATE;
    }

    private enum OperateType {
        UPDATE, CREATE
    }
}
