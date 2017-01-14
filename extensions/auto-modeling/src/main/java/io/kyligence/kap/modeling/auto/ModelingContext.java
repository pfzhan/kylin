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

package io.kyligence.kap.modeling.auto;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;

import io.kyligence.kap.source.hive.modelstats.ModelStats;

public class ModelingContext {
    private Map<String, TableExtDesc> tableExtDescs;
    private Map<String, TableDesc> tableDescs;
    private ModelStats modelStats;
    private DataModelDesc modelDesc;
    private KylinConfig kylinConfig;

    // config, such as weight value of different opt methods
    private double wPhyscal = 0.9;
    private double wBusiness = 0.1;

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

    public double getwPhyscal() {
        return wPhyscal;
    }

    public void setwPhyscal(double wPhyscal) {
        this.wPhyscal = wPhyscal;
    }

    public double getwBusiness() {
        return wBusiness;
    }

    public void setwBusiness(double wBusiness) {
        this.wBusiness = wBusiness;
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
        int colIdx = tableDescs.get(tblName).findColumnByName(colName).getZeroBasedIndex();
        return tableExtDescs.get(tblName).getColumnStats().get(colIdx);
    }

    public TableExtDesc.ColumnStats getTableColumnStats(TblColRef tblColRef) {
        return getTableColumnStats(tblColRef.getTable(), tblColRef.getName());
    }

    public long getColumnPairCardinality(List<String> cols) {
        Preconditions.checkState(cols != null && !cols.isEmpty());
        long result = 0;
        switch (cols.size()) {
        case 1:
            TblColRef colRef = modelDesc.findColumn(cols.get(0));
            result = getTableColumnStats(colRef).getCardinality();
            break;
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
                TblColRef colRef2 = modelDesc.findColumn(cols.get(0)); // FIXME why not use "col" but "cols.get(0)" ?
                result *= getTableColumnStats(colRef2).getCardinality();
            }
            break;
        }
        return result;
    }
}
