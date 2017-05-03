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

package io.kyligence.kap.modeling.smart;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.smart.common.ModelingConfig;
import io.kyligence.kap.modeling.smart.domain.Domain;
import io.kyligence.kap.modeling.smart.query.QueryStats;
import io.kyligence.kap.modeling.smart.stats.ICubeStats;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class ModelingContext {
    private ModelingConfig modelingConfig;

    private Map<String, TableExtDesc> tableExtDescs;
    private Map<String, TableDesc> tableDescs;
    private ModelStats modelStats;
    private DataModelDesc modelDesc;
    private KylinConfig kylinConfig;
    private QueryStats queryStats;
    private Domain domain;
    private String cubeName;
    private ICubeStats cubeStats;

    ModelingContext(ModelingConfig modelingConfig) {
        this.modelingConfig = modelingConfig;
    }

    public ModelingConfig getModelingConfig() {
        return modelingConfig;
    }

    public ICubeStats getCubeStats() {
        return cubeStats;
    }

    public void setCubeStats(CubeDesc cubeDesc, ICubeStats cubeStats) {
        this.cubeStats = cubeStats;
        if (modelStats == null && cubeDesc != null) {
            initModelStatsFromCubeStats(cubeDesc);
        }
    }

    private void initModelStatsFromCubeStats(CubeDesc cubeDesc) {
        ModelStats result = new ModelStats();
        Map<String, Long> singleColCardinality = Maps.newHashMap();
        Map<String, Long> doubleColCardinality = Maps.newHashMap();
        Map<Long, Long> cuboidRows = cubeStats.getCuboidRowsMap();

        RowKeyColDesc[] rowkeyDescCols = cubeDesc.getRowkey().getRowKeyColumns();

        Long cuboidId;
        Long rows;

        // get single column cardinality
        for (int i = 0; i < rowkeyDescCols.length; i++) {
            cuboidId = 1L << i;
            Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId); // if exact cuboid not existed, then use parent cuboid rows
            rows = cuboidRows.get(cuboid.getId());
            if (rows != null) {
                TblColRef rowkeyColRef = rowkeyDescCols[rowkeyDescCols.length - 1 - i].getColRef();
                singleColCardinality.put(rowkeyColRef.getIdentity(), rows);
            }
        }

        // get double column cardinality
        for (int i = 0; i < rowkeyDescCols.length; i++) {
            TblColRef rowkeyColRef1 = rowkeyDescCols[rowkeyDescCols.length - 1 - i].getColRef();
            for (int j = i + 1; j < rowkeyDescCols.length; j++) {
                cuboidId = (1L << i) | (1L << j);
                Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId); // if exact cuboid not existed, then use parent cuboid rows
                rows = cuboidRows.get(cuboid.getId());
                if (rows != null) {
                    TblColRef rowkeyColRef2 = rowkeyDescCols[rowkeyDescCols.length - 1 - j].getColRef();
                    String pairKey = rowkeyColRef1.getIdentity() + "," + rowkeyColRef2.getIdentity();
                    doubleColCardinality.put(pairKey, rows);
                }
            }
        }

        result.setDoubleColumnCardinality(doubleColCardinality);
        result.setSingleColumnCardinality(singleColCardinality);
        result.setModelName(cubeDesc.getModelName());

        this.modelStats = result;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    public QueryStats getQueryStats() {
        return queryStats;
    }

    public void setQueryStats(QueryStats queryStats) {
        this.queryStats = queryStats;
    }

    public DataModelDesc getModelDesc() {
        return modelDesc;
    }

    public void setModelDesc(DataModelDesc modelDesc) {
        this.modelDesc = modelDesc;
    }

    public KylinConfig getKylinConfig() {
        return kylinConfig;
    }

    public void setKylinConfig(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public ModelStats getModelStats() {
        return modelStats;
    }

    public void setModelStats(ModelStats modelStats) {
        this.modelStats = modelStats;
    }

    public Map<String, TableExtDesc> getTableExtDescs() {
        return tableExtDescs;
    }

    public void setTableExtDescs(Map<String, TableExtDesc> tableExtDescs) {
        this.tableExtDescs = tableExtDescs;
    }

    public Map<String, TableDesc> getTableDescs() {
        return tableDescs;
    }

    public void setTableDescs(Map<String, TableDesc> tableDescs) {
        this.tableDescs = tableDescs;
    }

    public boolean hasModelStats() {
        return modelStats != null;
    }

    public boolean hasQueryStats() {
        return queryStats != null;
    }

    public boolean hasTableStats() {
        return tableExtDescs != null && tableExtDescs.size() >= tableDescs.size();
    }

    public boolean hasCubeStats() {
        return cubeStats != null;
    }
    // ===================================

    public TableExtDesc.ColumnStats getTableColumnStats(TblColRef tblColRef) {
        String tableIdentity = tblColRef.getTable();
        int columnIdx = tblColRef.getColumnDesc().getZeroBasedIndex();
        if (tableExtDescs.containsKey(tableIdentity)) {
            List<ColumnStats> columnStats = tableExtDescs.get(tableIdentity).getColumnStats();
            if (columnStats != null && !columnStats.isEmpty()) {
                return columnStats.get(columnIdx);
            }
        }
        return null;
    }

    private TableExtDesc.ColumnStats getTableColumnStats(String tblName, String colName) {
        List<ColumnStats> colStats = tableExtDescs.get(tblName).getColumnStats();
        if (colStats != null && !colStats.isEmpty()) {
            int colIdx = tableDescs.get(tblName).findColumnByName(colName).getZeroBasedIndex();
            return colStats.get(colIdx);
        } else {
            return null;
        }
    }

    public long getColumnsCardinality(String col) {
        return getColumnsCardinality(Lists.newArrayList(col));
    }

    public long getColumnsCardinality(List<String> cols) {
        Preconditions.checkState(cols != null && !cols.isEmpty());
        long result = -1;
        switch (cols.size()) {
        case 1: {
            if (modelStats != null) {
                result = modelStats.getSingleColumnCardinalityVal(cols.get(0));
            }

            if (result < 0) {
                TblColRef colRef = modelDesc.findColumn(cols.get(0));
                TableExtDesc.ColumnStats colStats = getTableColumnStats(colRef);
                if (colStats != null) {
                    result = colStats.getCardinality();
                }
            }
            break;
        }
        case 2:
            if (modelStats != null) {
                result = modelStats.getDoubleColumnCardinalityVal(cols.get(0), cols.get(1));
                if (result >= 0) {
                    break;
                }
            }
        default:
            result = 1;
            for (String col : cols) {
                TblColRef colRef = modelDesc.findColumn(col);
                result *= getTableColumnStats(colRef).getCardinality();
            }
            break;
        }
        return result;
    }
}
