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

package io.kyligence.kap.smart.util;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.query.Utils;

public class CubeUtils {
    public static RowKeyColDesc getRowKeyColDescByName(CubeDesc cubeDesc, String rowkeyName) {
        TblColRef tblColRef = cubeDesc.getModel().findColumn(rowkeyName);
        return cubeDesc.getRowkey().getColDesc(tblColRef);
    }

    public static void fillCubeDefaultAdvSettings(CubeDesc cubeDesc) { // just default settings, not tuned
        RowKeyDesc rowkey = new RowKeyDesc();
        Set<RowKeyColDesc> rowKeyCols = Sets.newHashSet();
        for (DimensionDesc dimDesc : cubeDesc.getDimensions()) {
            dimDesc.init(cubeDesc);
            List<String> required = Lists.newArrayList();
            if (dimDesc.isDerived()) {
                Collections.addAll(required, dimDesc.getJoin().getForeignKey());
            } else {
                Collections.addAll(required, dimDesc.getTable() + "." + dimDesc.getColumn());
            }

            for (String col : required) {
                RowKeyColDesc rowKeyCol = new RowKeyColDesc();
                rowKeyCol.setColumn(col);
                rowKeyCol.setEncoding(DictionaryDimEnc.ENCODING_NAME);
                rowKeyCol.setShardBy(false);
                rowKeyCols.add(rowKeyCol);
            }
        }
        rowkey.setRowkeyColumns(rowKeyCols.toArray(new RowKeyColDesc[0]));
        cubeDesc.setRowkey(rowkey);

        // fill aggregation groups
        fillCubeDefaultAggGroups(cubeDesc);
        fillAggregationGroupsForMPCube(cubeDesc);

        Utils.setLargeCuboidCombinationConf(cubeDesc.getOverrideKylinProps());
        if (cubeDesc.getRowkey().getRowKeyColumns().length > 63) {
            Utils.setLargeRowkeySizeConf(cubeDesc.getOverrideKylinProps());
        }
    }

    public static void fillCubeDefaultAggGroups(CubeDesc cubeDesc) {
        AggregationGroup aggregationGroup = new AggregationGroup();
        List<String> includeCols = Lists.newArrayList();
        for (RowKeyColDesc rowKeyColDesc : cubeDesc.getRowkey().getRowKeyColumns()) {
            includeCols.add(rowKeyColDesc.getColumn());
        }
        aggregationGroup.setIncludes(includeCols.toArray(new String[0]));
        SelectRule selectRule = new SelectRule();
        selectRule.hierarchyDims = new String[0][0];
        selectRule.mandatoryDims = new String[0];//aggregationGroup.getIncludes();
        selectRule.jointDims = new String[0][0];
        aggregationGroup.setSelectRule(selectRule);

        cubeDesc.setAggregationGroups(Lists.newArrayList(aggregationGroup));
    }

    public static FunctionDesc newFunctionDesc(DataModelDesc modelDesc, String expression, ParameterDesc param,
            String colDataType) {
        String returnType = colDataType;
        switch (expression) {
        case FunctionDesc.FUNC_SUM:
            if (colDataType != null) {
                DataType type = DataType.getType(returnType);
                if (type.isIntegerFamily()) {
                    returnType = "bigint";
                } else if (type.isDecimal()) {
                    returnType = String.format("decimal(19,%d)", type.getScale());
                }
            } else {
                returnType = "decimal(19,4)";
            }
            break;
        case FunctionDesc.FUNC_COUNT:
            returnType = "bigint";
            break;
        case FunctionDesc.FUNC_COUNT_DISTINCT:
            returnType = SmartConfig.wrap(KylinConfig.getInstanceFromEnv()).getMeasureCountDistinctType(); //"hllc(10)";
            break;
        case FunctionDesc.FUNC_PERCENTILE:
            returnType = "percentile(100)";
            break;
        default:
            break;
        }

        FunctionDesc ret = FunctionDesc.newInstance(expression, param, returnType);
        ret.init(modelDesc);
        return ret;
    }

    public static FunctionDesc newCountStarFuncDesc(DataModelDesc modelDesc) {
        return newFunctionDesc(modelDesc, FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), "bigint");
    }

    public static NDataModel.Measure newMeasure(FunctionDesc func, String name, int id) {
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName(name);
        measure.setFunction(func);
        measure.id = NDataModel.MEASURE_ID_BASE;
        return measure;
    }

    public static DimensionDesc newDimensionDesc(TblColRef colRef) {
        DimensionDesc dimension = new DimensionDesc();
        dimension.setName(colRef.getIdentity());
        dimension.setTable(colRef.getTableAlias());
        dimension.setColumn(colRef.getName());
        return dimension;
    }

    public static void fillAggregationGroupsForMPCube(CubeDesc cubeDesc) {
        DataModelDesc modelDesc = cubeDesc.getModel();
        if (modelDesc instanceof KapModel) {
            KapModel kapModel = (KapModel) modelDesc;
            TblColRef[] mpCols = kapModel.getMutiLevelPartitionCols();

            for (AggregationGroup aggr : cubeDesc.getAggregationGroups()) {
                aggr.setIncludes(addColsToStrs(aggr.getIncludes(), mpCols));
                SelectRule selectRule = aggr.getSelectRule();
                selectRule.mandatoryDims = addColsToStrs(selectRule.mandatoryDims, mpCols);
            }
        }
    }

    private static String[] addColsToStrs(String[] strs, TblColRef[] mpCols) {
        if (strs == null)
            strs = new String[0];

        for (TblColRef col : mpCols) {
            String colStr = col.getIdentity();
            if (!ArrayUtils.contains(strs, colStr)) {
                strs = (String[]) ArrayUtils.add(strs, colStr);
            }
        }
        return strs;
    }
}
