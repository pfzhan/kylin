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

package io.kyligence.kap.smart.cube.proposer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.util.CubeDescUtil;

public class DerivedDimensionProposer extends AbstractCubeProposer {
    private static final Logger logger = LoggerFactory.getLogger(DerivedDimensionProposer.class);

    public DerivedDimensionProposer(CubeContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        if (context.hasTableStats() || context.hasModelStats()) { // tableStats and modelStats have cardinality information.
            int lastRowkeyNum = 0;
            int retry = 1;
            double derivedRatio = smartConfig.getDimDerivedRatio();
            do {
                lastRowkeyNum = workCubeDesc.getRowkey().getRowKeyColumns().length;

                buildDerivedDim(workCubeDesc, derivedRatio);
                CubeDescUtil.fillCubeDefaultAdvSettings(workCubeDesc);

                workCubeDesc.deInit();
                workCubeDesc.init(context.getKylinConfig());

                derivedRatio = derivedRatio / 2;
            } while (workCubeDesc.getRowkey().getRowKeyColumns().length > 63
                    && workCubeDesc.getRowkey().getRowKeyColumns().length < lastRowkeyNum
                    && retry++ < smartConfig.getDerivedStrictRetryMax());
        } else {
            logger.debug("No table stats or model stats found, skip proposing derived dimensions.");
        }
    }

    private void buildDerivedDim(CubeDesc workCubeDesc, double derivedRatio) {
        Set<DimensionDesc> normalDims = normalizeAllDerived(workCubeDesc);
        List<DimensionDesc> convertedDims = convertToDerived(workCubeDesc, normalDims, derivedRatio);
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
                for (TblColRef derivedCol : origDim.getColumnRefs()) {
                    DimensionDesc newDim = newDimensionDesc(workCubeDesc, derivedCol.getName(),
                            derivedCol.getTableAlias(), derivedCol.getIdentity(), null);
                    result.add(newDim);
                    colDimMap.put(derivedCol, newDim);
                }
            } else {
                result.add(origDim);
                colDimMap.put(origDim.getColumnRefs()[0], origDim);

            }
        }

        return result;
    }

    private List<DimensionDesc> convertToDerived(CubeDesc cubeDesc, Collection<DimensionDesc> origDimensions,
            double derivedRatio) {
        Set<DimensionDesc> workDimensions = Sets.newHashSet();
        DataModelDesc modelDesc = context.getModelDesc();

        // MP Columns must be set as normal
        Set<TblColRef> normalWhitelist = Sets.newHashSet();
        if (modelDesc instanceof KapModel) {
            Collections.addAll(normalWhitelist, ((KapModel) modelDesc).getMutiLevelPartitionCols());
        }

        // backup all FKs from joins
        Map<TblColRef, TableRef> allFKCols = Maps.newHashMap();
        for (JoinTableDesc joinTableDesc : modelDesc.getJoinTables()) {
            for (TblColRef fkCol : joinTableDesc.getJoin().getForeignKeyColumns()) {
                allFKCols.put(fkCol, joinTableDesc.getTableRef());
            }
        }

        // group dimensions from lookup tables by table
        Map<TableRef, List<DimensionDesc>> normalDimByTbl = Maps.newHashMap(); // all dimensions on lookup tables
        for (DimensionDesc origDim : origDimensions) {
            if (origDim.isDerived()) { // keep original derived dims
                workDimensions.add(origDim);
            } else {
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

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : normalDimByTbl.entrySet()) {
            TableRef tblRef = tblDims.getKey();
            List<String> derivedDimNames = Lists.newArrayList();

            // calculate cardinality of FKs for lookup table
            JoinDesc joinDesc = modelDesc.getJoinByPKSide(tblRef);
            TblColRef[] fkCols = joinDesc.getForeignKeyColumns();
            List<String> fkColNames = Lists.newArrayListWithExpectedSize(fkCols.length);
            for (TblColRef fkColRef : fkCols) {
                fkColNames.add(fkColRef.getIdentity());
            }
            long fKeyCardinality = context.getColumnsCardinality(fkColNames);

            // try to convert normal to derived one by one
            for (DimensionDesc tblDim : tblDims.getValue()) {
                TblColRef dimColRef = tblDim.getColumnRefs()[0]; // normal dimension only has 1 colRef
                if (!normalWhitelist.contains(dimColRef) && !allFKCols.containsKey(dimColRef)) {
                    long colCardinality = context.getColumnsCardinality(dimColRef.getIdentity());
                    double colCardRatio = (double) colCardinality / (double) fKeyCardinality;
                    if (colCardRatio > derivedRatio) {
                        logger.trace(
                                "Found one derived dimension: column={}, cardinality={}, pkCardinality={}, cardinalityRatio={}",
                                tblDim.getColumn(), colCardinality, fKeyCardinality, colCardRatio);
                        derivedDimNames.add(tblDim.getColumn());
                    } else {
                        workDimensions.add(tblDim);
                    }
                } else {
                    workDimensions.add(tblDim); // keep as normal
                }
            }

            if (!derivedDimNames.isEmpty()) {
                // only add one derived dim
                String tblAlias = tblRef.getAlias();
                String dimName = String.format("%s_%s", tblAlias, "DERIVED");
                workDimensions.add(
                        newDimensionDesc(cubeDesc, "{FK}", tblAlias, dimName, derivedDimNames.toArray(new String[0])));
            }
        }

        return Lists.newArrayList(workDimensions);
    }
}
