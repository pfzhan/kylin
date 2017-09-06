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

package io.kyligence.kap.modeling.smart.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.modeling.smart.util.CubeDescUtil;

public class Domain {
    private final DataModelDesc model;
    private final Set<TblColRef> dimensions;
    private final Set<FunctionDesc> measures;

    public Domain(DataModelDesc model, Set<TblColRef> dimensions, List<FunctionDesc> functionDescs) {
        this.model = model;
        this.dimensions = dimensions;

        measures = Sets.newHashSet();
        for (FunctionDesc funcDesc : functionDescs) {
            funcDesc.init(model);
            measures.add(funcDesc);
        }
    }

    public DataModelDesc getModel() {
        return model;
    }

    public Set<TblColRef> getDimensions() {
        return dimensions;
    }

    public Set<FunctionDesc> getMeasures() {
        return measures;
    }

    public CubeDesc buildCubeDesc() {
        KylinConfig kylinConfig = model.getConfig();

        CubeDesc cubeDesc = new CubeDesc();
        cubeDesc.setName(UUID.randomUUID().toString()); //random name assigned
        cubeDesc.updateRandomUuid();
        cubeDesc.setVersion(KylinVersion.getCurrentVersion().toString());
        cubeDesc.setModelName(model.getName());
        cubeDesc.setModel(model);
        cubeDesc.setEngineType(kylinConfig.getDefaultCubeEngine());
        cubeDesc.setStorageType(kylinConfig.getDefaultStorageEngine());

        fillDimensions(cubeDesc);
        fillMeasures(cubeDesc);
        CubeDescUtil.fillCubeDefaultAdvSettings(cubeDesc);

        return cubeDesc;
    }

    private void fillDimensions(CubeDesc cubeDesc) {
        // fill dimensions
        List<DimensionDesc> dimensions = new ArrayList<>();
        for (TblColRef colRef : this.getDimensions()) {
            DimensionDesc dimension = new DimensionDesc();
            dimension.setName(colRef.getIdentity());
            dimension.setTable(colRef.getTableAlias());
            dimension.setColumn(colRef.getName());
            dimensions.add(dimension);
        }
        cubeDesc.setDimensions(dimensions);
    }

    private void fillMeasures(CubeDesc cubeDesc) {
        Set<MeasureDesc> measureDescs = Sets.newHashSet();
        List<String> measureF1 = Lists.newArrayList();
        List<String> measureF2 = Lists.newArrayList();

        // Count * is a must include measure
        MeasureDesc countAll = new MeasureDesc();
        countAll.setName("_COUNT_");
        countAll.setFunction(CubeDescUtil.newFunctionDesc(model, "COUNT", ParameterDesc.newInstance("1"), "bigint"));
        measureDescs.add(countAll);

        // Column based measure function
        for (FunctionDesc measureFunc : measures) {
            measureFunc.init(model);
            MeasureDesc measureDesc = new MeasureDesc();
            measureDesc.setName(measureFunc.getParameter().getValue() + "_" + measureFunc.getExpression());
            measureDesc.setFunction(measureFunc);
            measureDescs.add(measureDesc);
        }
        
        List<MeasureDesc> measureSuggestion = new ArrayList<>(measureDescs);

        // Add to column family
        for (MeasureDesc measureDesc : measureSuggestion) {
            FunctionDesc measureFunc = measureDesc.getFunction();
            if (measureFunc.isCount() || measureFunc.isMax() || measureFunc.isMin() || measureFunc.isSum()) {
                measureF1.add(measureDesc.getName());
            } else {
                measureF2.add(measureDesc.getName());
            }
        }

        cubeDesc.setMeasures(measureSuggestion);

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
        cubeDesc.setHbaseMapping(hBaseMappingDesc);
    }
}
