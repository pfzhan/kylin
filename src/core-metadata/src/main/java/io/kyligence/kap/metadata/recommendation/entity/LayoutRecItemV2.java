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
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
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
    @JsonProperty("uuid")
    private String uuid;

    public int[] genDependIds() {
        List<Integer> colOrder = layout.getColOrder();
        int[] arr = new int[colOrder.size()];
        for (int i = 0; i < colOrder.size(); i++) {
            arr[i] = colOrder.get(i);
        }
        return arr;
    }

    public void updateLayoutInfo(NDataModel dataModel) {
        LayoutEntity layout = this.getLayout();
        Map<String, ComputedColumnDesc> ccMap = getCcOnModels(dataModel);
        ImmutableList<Integer> originColOrder = layout.getColOrder();
        List<Integer> originShardCols = layout.getShardByColumns();
        List<Integer> originSortCols = layout.getSortByColumns();
        List<Integer> originPartitionCols = layout.getPartitionByColumns();
        List<Integer> colOrderInDB = getColIDInDB(ccMap, dataModel, originColOrder);
        List<Integer> shardColsInDB = getColIDInDB(ccMap, dataModel, originShardCols);
        List<Integer> sortColsInDB = getColIDInDB(ccMap, dataModel, originSortCols);
        List<Integer> partitionColsInDB = getColIDInDB(ccMap, dataModel, originPartitionCols);
        layout.setColOrder(colOrderInDB);
        layout.setShardByColumns(shardColsInDB);
        layout.setSortByColumns(sortColsInDB);
        layout.setPartitionByColumns(partitionColsInDB);
        log.debug("Origin colOrder is {}, converted to {}", originColOrder, colOrderInDB);
        log.debug("Origin shardBy columns is {}, converted to {}", originShardCols, shardColsInDB);
        log.debug("Origin sortBy columns is {}, converted to {}", originSortCols, sortColsInDB);
        log.debug("Origin partition columns is {}, converted to {}", originPartitionCols, partitionColsInDB);
    }

    private List<Integer> getColIDInDB(Map<String, ComputedColumnDesc> ccMap, NDataModel model,
            List<Integer> columnIDs) {
        val uniqueRecItemMap = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .listAll();
        List<Integer> colOrderInDB = Lists.newArrayListWithCapacity(columnIDs.size());
        columnIDs.forEach(colId -> {
            String key;
            if (colId < NDataModel.MEASURE_ID_BASE) {
                TblColRef tblColRef = model.getEffectiveCols().get(colId);
                key = "d__" + getUniqueName(ccMap, tblColRef);
            } else {
                NDataModel.Measure measure = model.getEffectiveMeasures().get(colId);
                key = uniqueMeasureName(measure, ccMap);
            }
            if (uniqueRecItemMap.containsKey(key)) {
                colOrderInDB.add(-1 * uniqueRecItemMap.get(key).getId());
            } else {
                colOrderInDB.add(colId);
            }
        });
        return colOrderInDB;
    }

    private Map<String, ComputedColumnDesc> getCcOnModels(NDataModel model) {
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        model.getComputedColumnDescs().forEach(cc -> {
            String aliasDotName = cc.getTableAlias() + "." + cc.getColumnName();
            ccMap.putIfAbsent(aliasDotName, cc);
        });
        return ccMap;
    }

    private String uniqueMeasureName(NDataModel.Measure measure, Map<String, ComputedColumnDesc> ccMap) {
        Set<String> paramNames = Sets.newHashSet();
        List<ParameterDesc> parameters = measure.getFunction().getParameters();
        parameters.forEach(param -> {
            TblColRef colRef = param.getColRef();
            if (colRef == null) {
                paramNames.add(String.valueOf(Integer.MAX_VALUE));
                return;
            }
            paramNames.add(getUniqueName(ccMap, colRef));
        });
        return String.format("%s__%s", measure.getFunction().getExpression(), String.join("__", paramNames));
    }

    private String getUniqueName(Map<String, ComputedColumnDesc> ccMap, TblColRef tblColRef) {
        ColumnDesc columnDesc = tblColRef.getColumnDesc();
        String uniqueName;
        if (columnDesc.isComputedColumn()) {
            ComputedColumnDesc cc = ccMap.get(columnDesc.getIdentity());
            if (cc.getUuid() != null) {
                uniqueName = cc.getUuid();
            } else {
                uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getId();
            }
        } else {
            uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getId();
        }
        return uniqueName;
    }
}
