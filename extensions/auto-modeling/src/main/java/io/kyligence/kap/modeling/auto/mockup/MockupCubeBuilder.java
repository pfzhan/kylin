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

package io.kyligence.kap.modeling.auto.mockup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.TimeDimEnc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.modeling.auto.proposer.AnalyticsDomain;

public class MockupCubeBuilder {
    private String cubeName;

    /*
     * Initial analytics domain
     */
    private AnalyticsDomain domain;

    /*
     * Intermediate Info
     */
    // To Collect Dimension Info
    private List<DimensionDesc> dimensions;

    // To Collect RowKey Info
    private RowKeyDesc rowkey;

    // To Collect Aggregation-Group info
    private AggregationGroup aggGroup;

    // To Collect Measure Info
    private List<MeasureDesc> measures;

    // To Collect HBase Mapping Info
    private HBaseMappingDesc hBaseMapping;

    public MockupCubeBuilder(String cubeName, AnalyticsDomain domain) {
        this.domain = domain;
        this.cubeName = cubeName;
    }

    public CubeDesc build() {

        buildDimensions(domain.getDimensionCols());

        buildMeasures(domain.getMeasureFuncs());

        DataModelDesc model = domain.getModel();

        // Assemble
        CubeDesc cubeDesc = new CubeDesc();
        cubeDesc.updateRandomUuid();
        // cubeDesc.setLastModified(System.currentTimeMillis());
        cubeDesc.setVersion(KylinVersion.getCurrentVersion().toString());
        cubeDesc.setName(cubeName);
        cubeDesc.setModelName(model.getName());
        cubeDesc.setModel(model);
        cubeDesc.setDimensions(dimensions);
        cubeDesc.setMeasures(measures);
        cubeDesc.setRowkey(rowkey);
        cubeDesc.setAggregationGroups(Arrays.asList(new AggregationGroup[] { aggGroup }));
        cubeDesc.setHbaseMapping(hBaseMapping);
        cubeDesc.setEngineType(KylinConfig.getInstanceFromEnv().getDefaultCubeEngine());
        cubeDesc.setStorageType(70);
        return cubeDesc;
    }

    private void buildDimensions(List<TblColRef> dimensionCols) {
        List<DimensionDesc> dimensions = new ArrayList<>();
        for (TblColRef colRef : dimensionCols) {
            DimensionDesc dimension = new DimensionDesc();
            dimension.setName(colRef.getName());
            dimension.setTable(colRef.getTableAlias());
            dimension.setColumn(colRef.getName());
            dimensions.add(dimension);
        }
        this.dimensions = dimensions;

        RowKeyDesc rowkey = new RowKeyDesc();
        List<RowKeyColDesc> rowKeyCols = new ArrayList<>();
        for (TblColRef colRef : dimensionCols) {
            RowKeyColDesc rowKeyCol = new RowKeyColDesc();
            rowKeyCol.setColumn(colRef.getIdentity());
            if (colRef.getType().isDate()) {
                rowKeyCol.setEncoding(DateDimEnc.ENCODING_NAME);
            } else if (colRef.getType().isTimeFamily()) {
                rowKeyCol.setEncoding(TimeDimEnc.ENCODING_NAME);
            } else {
                rowKeyCol.setEncoding(DictionaryDimEnc.ENCODING_NAME);
            }
            rowKeyCol.setShardBy(false);
            rowKeyCols.add(rowKeyCol);
        }
        rowkey.setRowkeyColumns(rowKeyCols.toArray(new RowKeyColDesc[0]));
        this.rowkey = rowkey;

        AggregationGroup aggregationGroup = new AggregationGroup();
        List<String> includeCols = new ArrayList<>(rowKeyCols.size());
        for (RowKeyColDesc rowKeyColDesc : rowKeyCols) {
            includeCols.add(rowKeyColDesc.getColumn());
        }
        aggregationGroup.setIncludes(includeCols.toArray(new String[0]));
        SelectRule selectRule = new SelectRule();
        selectRule.hierarchy_dims = new String[0][0];
        selectRule.mandatory_dims = new String[0];
        selectRule.joint_dims = new String[0][0];
        aggregationGroup.setSelectRule(selectRule);
        this.aggGroup = aggregationGroup;
    }

    private void buildMeasures(Set<FunctionDesc> measureFuncs) {
        List<MeasureDesc> measures = new ArrayList<>();
        List<String> measureF1 = new ArrayList<>();
        List<String> measureF2 = new ArrayList<>();

        // Count * is a must include measure
        MeasureDesc countAll = new MeasureDesc();
        countAll.setName("_COUNT_");
        countAll.setFunction(FunctionDesc.newInstance("COUNT", ParameterDesc.newInstance("1"), "bigint"));
        measures.add(countAll);
        measureF1.add(countAll.getName());

        // Column based measure function
        for (FunctionDesc measureFunc : measureFuncs) {
            if (measureFunc.isCountDistinct()) {
                MeasureDesc colCountDist = new MeasureDesc();
                colCountDist.setName(measureFunc.getParameter().getValue() + "_COUNT");
                colCountDist.setFunction(measureFunc);
                measures.add(colCountDist);
                measureF2.add(colCountDist.getName());
            } else if (measureFunc.isMax() || measureFunc.isMin() || measureFunc.isSum()) {
                MeasureDesc colCountDist = new MeasureDesc();
                colCountDist.setName(measureFunc.getParameter().getValue() + "_" + measureFunc.getExpression());
                colCountDist.setFunction(measureFunc);
                measures.add(colCountDist);
                measureF1.add(colCountDist.getName());
            }
        }
        this.measures = measures;

        // setup hbase mapping
        int cfNum = 1;
        if (!measureF2.isEmpty()) {
            cfNum = 2;
        }
        HBaseMappingDesc hBaseMappingDesc = new HBaseMappingDesc();
        HBaseColumnFamilyDesc[] columnFamily = new HBaseColumnFamilyDesc[cfNum];
        // Hbase mapping F1
        columnFamily[0] = new HBaseColumnFamilyDesc();
        columnFamily[0].setName("F1");
        HBaseColumnDesc columnF1 = new HBaseColumnDesc();
        columnF1.setQualifier("M");
        columnF1.setMeasureRefs(measureF1.toArray(new String[0]));
        columnFamily[0].setColumns(new HBaseColumnDesc[] { columnF1 });

        if (cfNum == 2) {
            // Hbase mapping F2
            columnFamily[1] = new HBaseColumnFamilyDesc();
            columnFamily[1].setName("F2");
            HBaseColumnDesc columnF2 = new HBaseColumnDesc();
            columnF2.setQualifier("M");
            columnF2.setMeasureRefs(measureF2.toArray(new String[0]));
            columnFamily[1].setColumns(new HBaseColumnDesc[] { columnF2 });
        }
        hBaseMappingDesc.setColumnFamily(columnFamily);
        this.hBaseMapping = hBaseMappingDesc;
    }
}
