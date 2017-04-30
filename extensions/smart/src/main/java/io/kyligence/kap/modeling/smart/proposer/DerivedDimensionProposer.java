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

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
        if (context.hasTableStats() || context.hasModelStats()) { // tableStats and modelStats have cardinality information.
            buildDerivedDim(workCubeDesc);
            CubeDescUtil.fillCubeDefaultAdvSettings(workCubeDesc);

            workCubeDesc.deInit();
            workCubeDesc.init(context.getKylinConfig());
        }
    }

    private void buildDerivedDim(CubeDesc workCubeDesc) {
        Set<DimensionDesc> normalDims = normalizeAllDerived(workCubeDesc);
        List<DimensionDesc> convertedDims = convertToDerived(workCubeDesc, normalDims);
        workCubeDesc.setDimensions(convertedDims);
    }

    private DimensionDesc newDimensionDesc(CubeDesc cubeDesc, String col, String tbl, String name, String[] derived) {
        DimensionDesc newDim = new DimensionDesc();
        newDim.setColumn(col);
        newDim.setDerived(derived);
        newDim.setTable(tbl);
        newDim.setName(name);
        newDim.init(cubeDesc);
        return newDim;
    }

    private Set<DimensionDesc> normalizeAllDerived(CubeDesc workCubeDesc) {
        Set<DimensionDesc> result = Sets.newHashSet();
        Map<TblColRef, DimensionDesc> colDimMap = Maps.newHashMap();
        List<DimensionDesc> origDims = workCubeDesc.getDimensions();
        for (DimensionDesc origDim : origDims) {
            if (origDim.isDerived()) {
                //                for (TblColRef fkCol : origDim.getJoin().getForeignKeyColumns()) {
                //                    result.add(newDimensionDesc(workCubeDesc, fkCol.getName(), fkCol.getTableAlias(), fkCol.getIdentity(), null));
                //                }
                for (TblColRef derivedCol : origDim.getColumnRefs()) {
                    DimensionDesc newDim = newDimensionDesc(workCubeDesc, derivedCol.getName(), derivedCol.getTableAlias(), derivedCol.getIdentity(), null);
                    result.add(newDim);
                    colDimMap.put(derivedCol, newDim);
                }
            } else {
                result.add(origDim);
                colDimMap.put(origDim.getColumnRefs()[0], origDim);

            }
        }

        // remove dup PK/FK from inner join tables
        for (JoinTableDesc joinTableDesc : context.getModelDesc().getJoinTables()) {
            JoinDesc joinDesc = joinTableDesc.getJoin();
            if (joinDesc.isInnerJoin()) {
                TblColRef[] fks = joinDesc.getForeignKeyColumns();
                TblColRef[] pks = joinDesc.getPrimaryKeyColumns();
                for (int i = 0; i < fks.length; i++) {
                    DimensionDesc fkDim = colDimMap.get(fks[i]);
                    DimensionDesc pkDim = colDimMap.get(pks[i]);
                    if (pkDim != null) {
                        result.remove(pkDim);
                    }
                    if (fkDim == null) {
                        DimensionDesc newDim = newDimensionDesc(workCubeDesc, fks[i].getName(), fks[i].getTableAlias(), fks[i].getIdentity(), null);
                        result.add(newDim);
                    }
                }
            }
        }
        return result;
    }

    private List<DimensionDesc> convertToDerived(CubeDesc cubeDesc, Collection<DimensionDesc> origDimensions) {
        Set<DimensionDesc> workDimensions = Sets.newHashSet();
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
            } else {
                // if a lookup has derived dims, the FK should not be declared as normal, as the derived dim already contains it
                TblColRef tblColRef = origDim.getTableRef().getColumn(origDim.getColumn()); // origDim.getColumnRefs() not initialized

                // check if this is a FK col.
                TableRef fkTblRef = allFKCols.get(tblColRef);
                if (fkTblRef != null) {
                    // this is a FK col, just record it, and will remove if there's derived dim on this lookup table
                    if (!fkDimsByTbl.containsKey(fkTblRef)) {
                        fkDimsByTbl.put(fkTblRef, Lists.<DimensionDesc> newArrayList());
                    }
                    fkDimsByTbl.get(fkTblRef).add(origDim);
                } else {
                    // not a FK col, record as derived candidate
                    TableRef tblRef = origDim.getTableRef();
                    if (modelDesc.getRootFactTable() != tblRef) {
                        if (!normalDimByTbl.containsKey(tblRef)) {
                            normalDimByTbl.put(tblRef, Lists.<DimensionDesc> newArrayList());
                        }
                        normalDimByTbl.get(tblRef).add(origDim);
                    } else {
                        // fact dim, keep as normal
                        workDimensions.add(origDim);
                    }
                }
            }
        }

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : normalDimByTbl.entrySet()) {
            // if there's only 1 dim on this table, immediately set as normal
            //            if (tblDims.getValue().size() == 1) {
            //                workDimensions.add(tblDims.getValue().get(0));
            //                continue;
            //            }

            TableRef tblRef = tblDims.getKey();
            String tblAlias = tblRef.getAlias();
            List<String> derivedDimNames = Lists.newArrayList();
            JoinDesc joinDesc = modelDesc.getJoinByPKSide(tblRef);

            TblColRef[] pkCols = joinDesc.getPrimaryKeyColumns();
            List<DimensionDesc> pKeyDimsBackup = Lists.newArrayList();

            // calculate possible cardinality of PKs on lookup table
            // TODO: get from model stats and cube stats
            long pKeyCardinality = 1;
            for (TblColRef pkCol : pkCols) {
                long cardinality = context.getColumnsCardinality(pkCol.getIdentity());
                if (cardinality > 0) {
                    pKeyCardinality = pKeyCardinality * cardinality;
                }
            }

            for (DimensionDesc tblDim : tblDims.getValue()) {
                TblColRef dimColRef = tblDim.getColumnRefs()[0]; // normal dimension only has 1 colRef
                if (ArrayUtils.contains(pkCols, dimColRef)) {
                    // save pk dimensions, will add them as normal if no derived exists
                    pKeyDimsBackup.add(tblDim);
                } else {
                    long colCardinality = context.getColumnsCardinality(dimColRef.getIdentity());
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
                workDimensions.add(newDimensionDesc(cubeDesc, Constants.DIM_DEREIVED_COLUMN_NAME, tblAlias, dimName, derivedDimNames.toArray(new String[0])));
            } else if (!pKeyDimsBackup.isEmpty()) {
                workDimensions.addAll(fkDimsByTbl.get(tblRef));
                workDimensions.addAll(pKeyDimsBackup);
            }
        }

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : fkDimsByTbl.entrySet()) {
            if (!normalDimByTbl.containsKey(tblDims.getKey())) {
                workDimensions.addAll(tblDims.getValue());
            }
        }
        return Lists.newArrayList(workDimensions);
    }
}
