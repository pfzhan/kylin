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

package io.kyligence.kap.modeling.smart.proposer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.util.Constants;
import io.kyligence.kap.modeling.smart.util.CubeDescUtil;

public class DerivedDimensionProposer extends AbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(DerivedDimensionProposer.class);

    public DerivedDimensionProposer(ModelingContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        buildDerivedDim(workCubeDesc);
        CubeDescUtil.fillCubeDefaultAdvSettings(workCubeDesc);

        workCubeDesc.deInit();
        workCubeDesc.init(context.getKylinConfig());
    }

    private void buildDerivedDim(CubeDesc workCubeDesc) {
        Set<DimensionDesc> normalDims = normalizeAllDerived(workCubeDesc);
        List<DimensionDesc> convertedDims = convertToDerived(normalDims);
        workCubeDesc.setDimensions(convertedDims);
    }

    private DimensionDesc newDimensionDesc(String col, String tbl, String name, String[] derived) {
        DimensionDesc newDim = new DimensionDesc();
        newDim.setColumn(col);
        newDim.setDerived(derived);
        newDim.setTable(tbl);
        newDim.setName(name);
        return newDim;
    }

    private Set<DimensionDesc> normalizeAllDerived(CubeDesc workCubeDesc) {
        Set<DimensionDesc> result = Sets.newHashSet();
        List<DimensionDesc> origDims = workCubeDesc.getDimensions();
        for (DimensionDesc origDim : origDims) {
            if (origDim.isDerived()) {
                for (TblColRef fkCol : origDim.getJoin().getForeignKeyColumns()) {
                    result.add(newDimensionDesc(fkCol.getIdentity(), fkCol.getTableAlias(), fkCol.getIdentity(), null));
                }
                for (TblColRef derivedCol : origDim.getColumnRefs()) {
                    result.add(newDimensionDesc(derivedCol.getIdentity(), derivedCol.getTableAlias(), derivedCol.getIdentity(), null));
                }
            } else {
                result.add(origDim);
            }
        }
        return result;
    }

    private List<DimensionDesc> convertToDerived(Collection<DimensionDesc> origDimensions) {
        List<DimensionDesc> workDimensions = Lists.newArrayList();
        DataModelDesc modelDesc = context.getModelDesc();

        // backup all FKs from joins
        Map<TblColRef, TableRef> allFKCols = Maps.newHashMap();
        for (JoinTableDesc joinTableDesc : modelDesc.getJoinTables()) {
            for (TblColRef fkCol : joinTableDesc.getJoin().getForeignKeyColumns()) {
                allFKCols.put(fkCol, joinTableDesc.getTableRef());
            }
        }

        Map<TableRef, List<DimensionDesc>> normalDimByTbl = Maps.newHashMap(); // all dimensions on lookup tables
        Map<TableRef, List<DimensionDesc>> fkDimsByTbl = Maps.newHashMap();
        for (DimensionDesc origDim : origDimensions) {
            if (origDim.isDerived()) { // keep derived dims
                workDimensions.add(origDim);
            } else if (modelDesc.isFactTable(origDim.getTableRef())) {
                // if a lookup has derived dims, the FK should not be declared as normal, as the derived dim already contains it
                TblColRef tblColRef = origDim.getColumnRefs()[0];
                TableRef tblRef = allFKCols.get(tblColRef);
                if (tblRef != null) {
                    if (!fkDimsByTbl.containsKey(tblRef)) {
                        fkDimsByTbl.put(tblRef, Lists.<DimensionDesc> newArrayList());
                    }
                    fkDimsByTbl.get(tblRef).add(origDim);
                } else {
                    workDimensions.add(origDim);
                }
            } else {
                TableRef tblRef = origDim.getTableRef();
                if (!normalDimByTbl.containsKey(tblRef)) {
                    normalDimByTbl.put(tblRef, Lists.<DimensionDesc> newArrayList());
                }
                normalDimByTbl.get(tblRef).add(origDim);
            }
        }

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : normalDimByTbl.entrySet()) {
            TableRef tblRef = tblDims.getKey();
            String tblAlias = tblRef.getAlias();
            String tblName = tblRef.getTableIdentity();
            List<String> derivedDimNames = Lists.newArrayList();
            JoinDesc joinDesc = modelDesc.getJoinByPKSide(tblRef);

            String[] pKeys = joinDesc.getPrimaryKey();
            TblColRef[] pkCols = joinDesc.getPrimaryKeyColumns();
            List<DimensionDesc> pKeyDimsBackup = Lists.newArrayList();

            // calculate possible cardinality of PKs on lookup table
            // TODO: get from model stats and cube stats
            long pKeyCardinality = 1;
            for (String pKey : pKeys) {
                pKeyCardinality = pKeyCardinality * context.getTableColumnStats(tblName, pKey).getCardinality();
            }

            for (DimensionDesc tblDim : tblDims.getValue()) {
                if (ArrayUtils.contains(pkCols, tblDim.getColumnRefs()[0])) { // normal dimension only has 1 colRef
                    // save pk dimensions, will add them as normal if no derived exists
                    pKeyDimsBackup.add(tblDim);
                } else {
                    TableExtDesc.ColumnStats colStats = context.getTableColumnStats(tblName, tblDim.getColumn());
                    long colCardinality = colStats.getCardinality();
                    double colCardRatio = (double) colCardinality / (double) pKeyCardinality;
                    if (colCardRatio > Constants.DIM_DERIVED_PK_RATIO) {
                        logger.debug("Found one derived dimension: column={}, cardinality={}, pkCardinality={}, cardinalityRatio={}", tblDim.getColumn(), colCardinality, pKeyCardinality, colCardRatio);
                        derivedDimNames.add(tblDim.getColumn());
                    } else {
                        workDimensions.add(tblDim);
                    }
                }
            }

            if (!derivedDimNames.isEmpty()) {
                // only add one derived dim, ignore all PK and FK dims
                String dimName = String.format("%s_%s", tblAlias, Constants.DIM_DEREIVED_NAME_SUFFIX);
                workDimensions.add(newDimensionDesc(Constants.DIM_DEREIVED_COLUMN_NAME, tblAlias, dimName, derivedDimNames.toArray(new String[0])));
            } else if (!pKeyDimsBackup.isEmpty()) {
                // TODO: remove dup from FK and PK
                workDimensions.addAll(fkDimsByTbl.get(tblRef));
                workDimensions.addAll(pKeyDimsBackup);
            }
        }

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : fkDimsByTbl.entrySet()) {
            if (!normalDimByTbl.containsKey(tblDims.getKey())) {
                workDimensions.addAll(tblDims.getValue());
            }
        }
        return workDimensions;
    }
}
