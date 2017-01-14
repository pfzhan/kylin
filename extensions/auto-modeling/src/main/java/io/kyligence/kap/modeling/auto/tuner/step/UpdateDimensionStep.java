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

package io.kyligence.kap.modeling.auto.tuner.step;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.auto.ModelingContext;
import io.kyligence.kap.modeling.auto.util.Constants;

public class UpdateDimensionStep extends AbstractStep {
    private static final Logger logger = LoggerFactory.getLogger(UpdateDimensionStep.class);

    public UpdateDimensionStep(ModelingContext context, CubeDesc origCubeDesc, CubeDesc workCubeDesc) {
        super(context, origCubeDesc, workCubeDesc);
    }

    @Override
    void doOptimize() {
        convertToDerived();
    }

    @Override
    void afterOptimize() throws Exception {
        adjustAfterDimensionChanged();
        super.afterOptimize();
    }

    private void convertToDerived() {
        List<DimensionDesc> origDimensions = origCubeDesc.getDimensions();
        List<DimensionDesc> workDimensions = Lists.newArrayList();
        DataModelDesc modelDesc = origCubeDesc.getModel();
        Map<TableRef, List<DimensionDesc>> normalDimByTbl = Maps.newHashMap();
        for (DimensionDesc origDim : origDimensions) {
            if (origDim.isDerived() || modelDesc.isFactTable(origDim.getTableRef())) {
                workDimensions.add(origDim);
                continue;
            }

            TableRef tblRef = origDim.getTableRef();
            if (!normalDimByTbl.containsKey(tblRef)) {
                normalDimByTbl.put(tblRef, Lists.<DimensionDesc> newArrayList());
            }
            normalDimByTbl.get(tblRef).add(origDim);
        }

        for (Map.Entry<TableRef, List<DimensionDesc>> tblDims : normalDimByTbl.entrySet()) {
            TableRef tblRef = tblDims.getKey();
            String tblAlias = tblRef.getAlias();
            String tblName = tblRef.getTableIdentity();
            List<String> derivedDimNames = Lists.newArrayList();
            JoinDesc joinDesc = modelDesc.getJoinByPKSide(tblRef);
            String[] pKeys = joinDesc.getPrimaryKey();
            Map<String, List<DimensionDesc>> pKeyDimsBackup = Maps.newHashMap();

            // calculate possible cardinality of PKs on lookup table
            long pKeyCardinality = 1;
            for (String pKey : pKeys) {
                pKeyCardinality = pKeyCardinality * context.getTableColumnStats(tblName, pKey).getCardinality();
            }

            for (DimensionDesc tblDim : tblDims.getValue()) {
                if (ArrayUtils.contains(pKeys, tblDim.getTable() + "." + tblDim.getColumn())) {
                    // save pk dimensions, will add them as normal if no derived exists
                    if (!pKeyDimsBackup.containsKey(tblDim.getTable())) {
                        pKeyDimsBackup.put(tblDim.getTable(), Lists.<DimensionDesc> newArrayList());
                    }
                    pKeyDimsBackup.get(tblDim.getTable()).add(tblDim);
                    continue;
                }

                TableExtDesc.ColumnStats colStats = context.getTableColumnStats(tblName, tblDim.getColumn());
                long colCardinality = colStats.getCardinality();
                double colCardRatio = (double) colCardinality / (double) pKeyCardinality;
                if (colCardRatio > Constants.DIM_DERIVED_PK_RATIO) {
                    // add to derived dimensions
                    logger.debug("Found one derived dimension: column={}, cardinality={}, pkCardinality={}, cardinalityRatio={}", tblDim.getColumn(), colCardinality, pKeyCardinality, colCardRatio);
                    derivedDimNames.add(tblDim.getColumn());
                } else {
                    // still keep as normal dimensions
                    workDimensions.add(tblDim);
                }
            }

            if (!derivedDimNames.isEmpty()) {
                // has derived dimensions
                DimensionDesc newDerived = new DimensionDesc();
                newDerived.setColumn(Constants.DIM_DEREIVED_COLUMN_NAME);
                newDerived.setTable(tblAlias);
                newDerived.setName(String.format("%s_%s", tblName, Constants.DIM_DEREIVED_NAME_SUFFIX));
                newDerived.setDerived(derivedDimNames.toArray(new String[0]));
                newDerived.init(workCubeDesc);
                workDimensions.add(newDerived);
            } else if (pKeyDimsBackup.get(tblName) != null) {
                workDimensions.addAll(pKeyDimsBackup.get(tblName));
            }
        }

        workCubeDesc.setDimensions(workDimensions);
    }
}
