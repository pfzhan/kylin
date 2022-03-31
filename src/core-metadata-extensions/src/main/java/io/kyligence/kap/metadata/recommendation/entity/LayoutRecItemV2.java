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

package io.kyligence.kap.metadata.recommendation.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LayoutRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("layout")
    private LayoutEntity layout;
    @JsonProperty("is_agg")
    private boolean isAgg;

    public int[] genDependIds() {
        List<Integer> colOrder = layout.getColOrder();
        int[] arr = new int[colOrder.size()];
        for (int i = 0; i < colOrder.size(); i++) {
            arr[i] = colOrder.get(i);
        }
        return arr;
    }

    public void updateLayoutContent(NDataModel dataModel, Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        Map<String, ComputedColumnDesc> ccMap = getCcNameMapOnModel(dataModel);
        Map<String, RawRecItem> uniqueContentRecMap = Maps.newHashMap();
        nonLayoutUniqueFlagRecMap.forEach((uniqueFlag, recItem) -> {
            if (recItem.getModelID().equalsIgnoreCase(dataModel.getUuid())) {
                uniqueContentRecMap.put(recItem.getRecEntity().getUniqueContent(), recItem);
            }
        });

        ImmutableList<Integer> originColOrder = layout.getColOrder();
        List<Integer> originShardCols = layout.getShardByColumns();
        List<Integer> originSortCols = layout.getSortByColumns();
        List<Integer> originPartitionCols = layout.getPartitionByColumns();
        List<Integer> colOrderInDB = getColIDInDB(ccMap, dataModel, originColOrder, uniqueContentRecMap);
        List<Integer> shardColsInDB = getColIDInDB(ccMap, dataModel, originShardCols, uniqueContentRecMap);
        List<Integer> sortColsInDB = getColIDInDB(ccMap, dataModel, originSortCols, uniqueContentRecMap);
        List<Integer> partitionColsInDB = getColIDInDB(ccMap, dataModel, originPartitionCols, uniqueContentRecMap);
        layout.setColOrder(colOrderInDB);
        layout.setShardByColumns(shardColsInDB);
        layout.setPartitionByColumns(partitionColsInDB);
        log.debug("Origin colOrder is {}, converted to {}", originColOrder, colOrderInDB);
        log.debug("Origin shardBy columns is {}, converted to {}", originShardCols, shardColsInDB);
        log.debug("Origin sortBy columns is {}, converted to {}", originSortCols, sortColsInDB);
        log.debug("Origin partition columns is {}, converted to {}", originPartitionCols, partitionColsInDB);
    }

    private List<Integer> getColIDInDB(Map<String, ComputedColumnDesc> ccNameMap, NDataModel model,
            List<Integer> columnIDs, Map<String, RawRecItem> uniqueContentToRecItemMap) {
        List<Integer> colOrderInDB = Lists.newArrayListWithCapacity(columnIDs.size());
        columnIDs.forEach(colId -> {
            String uniqueContent;
            if (colId < NDataModel.MEASURE_ID_BASE) {
                TblColRef tblColRef = model.getEffectiveCols().get(colId);
                uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, ccNameMap);
            } else {
                NDataModel.Measure measure = model.getEffectiveMeasures().get(colId);
                uniqueContent = RawRecUtil.measureUniqueContent(measure, ccNameMap);
            }
            if (uniqueContentToRecItemMap.containsKey(uniqueContent)) {
                colOrderInDB.add(-uniqueContentToRecItemMap.get(uniqueContent).getId());
            } else {
                colOrderInDB.add(colId);
            }
        });
        return colOrderInDB;
    }

    private Map<String, ComputedColumnDesc> getCcNameMapOnModel(NDataModel model) {
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        model.getComputedColumnDescs().forEach(cc -> {
            String aliasDotName = cc.getTableAlias() + "." + cc.getColumnName();
            ccMap.putIfAbsent(aliasDotName, cc);
        });
        return ccMap;
    }
}
