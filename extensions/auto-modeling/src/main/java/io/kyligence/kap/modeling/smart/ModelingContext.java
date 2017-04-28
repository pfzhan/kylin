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
import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.smart.domain.Domain;
import io.kyligence.kap.modeling.smart.query.QueryStats;
import io.kyligence.kap.modeling.smart.stats.ICubeStats;
import io.kyligence.kap.modeling.smart.util.Constants;
import io.kyligence.kap.modeling.smart.util.MathUtil;
import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class ModelingContext {
    private Map<String, TableExtDesc> tableExtDescs;
    private Map<String, TableDesc> tableDescs;
    private ModelStats modelStats;
    private DataModelDesc modelDesc;
    private KylinConfig kylinConfig;
    private QueryStats queryStats;
    private Domain domain;
    private String cubeName;
    private ICubeStats cubeStats;

    // config, such as weight value of different opt methods
    private double physcalCoe = Constants.COE_PHYSCAL;
    private double businessCoe = Constants.COE_BUSINESS;

    public ICubeStats getCubeStats() {
        return cubeStats;
    }

    public void setCubeStats(CubeDesc cubeDesc, ICubeStats cubeStats) {
        this.cubeStats = cubeStats;
        if (modelStats == null && cubeDesc != null) {
            initModelStatsFromCubeStats(cubeDesc);
        }
    }

    private String normTblColRefId(TblColRef tblColRef) {
        return tblColRef.getTableRef().getTableDesc().getDatabase() + "." + tblColRef.getIdentity();
    }

    private void initModelStatsFromCubeStats(CubeDesc cubeDesc) {
        ModelStats result = new ModelStats();
        Map<String, Long> singleColCardinality = Maps.newHashMap();
        Map<String, Long> doubleColCardinality = Maps.newHashMap();
        Map<Long, Long> cuboidRows = cubeStats.getCuboidRowsMap();
        for (Map.Entry<Long, Long> cuboidRow : cuboidRows.entrySet()) {
            long cuboidId = cuboidRow.getKey();
            int cntBitOne = MathUtil.countBitOne(cuboidId);
            switch (cntBitOne) {
            case 1: {
                Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
                if (cuboid.getColumns().size() == 1) {
                    TblColRef tblColRef = cuboid.getColumns().get(0);
                    singleColCardinality.put(normTblColRefId(tblColRef), cuboidRow.getValue());
                }
                break;
            }
            case 2: {
                Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
                if (cuboid.getColumns().size() == 2) {
                    TblColRef tblColRef1 = cuboid.getColumns().get(0);
                    TblColRef tblColRef2 = cuboid.getColumns().get(1);
                    String pairKey = normTblColRefId(tblColRef1) + "," + normTblColRefId(tblColRef2);
                    doubleColCardinality.put(pairKey, cuboidRow.getValue());
                }
                break;
            }
            default:
                break;
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

    public double getPhyscalCoe() {
        return physcalCoe;
    }

    public void setPhyscalCoe(double physcalCoe) {
        this.physcalCoe = physcalCoe;
    }

    public double getBusinessCoe() {
        return businessCoe;
    }

    public void setBusinessCoe(double businessCoe) {
        this.businessCoe = businessCoe;
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

    public TableExtDesc.ColumnStats getRowKeyColumnStats(RowKeyColDesc rowKeyColDesc) {
        String tableIdentity = rowKeyColDesc.getColRef().getTable();
        int columnIdx = rowKeyColDesc.getColRef().getColumnDesc().getZeroBasedIndex();
        if (tableExtDescs.containsKey(tableIdentity)) {
            List<ColumnStats> columnStats = tableExtDescs.get(tableIdentity).getColumnStats();
            if (columnStats != null && !columnStats.isEmpty()) {
                return columnStats.get(columnIdx);
            }
        }
        return null;
    }

    public TableExtDesc.ColumnStats getTableColumnStats(String tblName, String colName) {
        List<ColumnStats> colStats = tableExtDescs.get(tblName).getColumnStats();
        if (colStats != null && !colStats.isEmpty()) {
            int colIdx = tableDescs.get(tblName).findColumnByName(colName).getZeroBasedIndex();
            return colStats.get(colIdx);
        } else {
            return null;
        }
    }

    public TableExtDesc.ColumnStats getTableColumnStats(TblColRef tblColRef) {
        return getTableColumnStats(tblColRef.getTable(), tblColRef.getName());
    }

    public long getColumnsCardinality(List<String> cols) {
        Preconditions.checkState(cols != null && !cols.isEmpty());
        long result = 0;
        switch (cols.size()) {
        case 1: {
            result = modelStats.getSingleColumnCardinalityVal(cols.get(0));
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
