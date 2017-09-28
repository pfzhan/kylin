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

package io.kyligence.kap.smart.cube.domain;

import java.util.List;
import java.util.Set;

import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.util.CubeDescUtil;

public class ModelDomainBuilder implements IDomainBuilder {
    private final DataModelDesc modelDesc;

    public ModelDomainBuilder(DataModelDesc modelDesc) {
        Preconditions.checkNotNull(modelDesc);
        Preconditions.checkNotNull(modelDesc.getAllTables());

        this.modelDesc = modelDesc;
    }

    @Override
    public Domain build() {
        // Setup dimensions
        Set<TblColRef> dimensionCols = Sets.newHashSet();
        for (ModelDimensionDesc dim : modelDesc.getDimensions()) {
            TableRef lookupTable = modelDesc.findTable(dim.getTable());
            for (String col : dim.getColumns()) {
                TblColRef colRef = lookupTable.getColumn(col);
                if (colRef != null) {
                    dimensionCols.add(colRef);
                }
            }
        }

        // Setup measures
        List<TblColRef> measureCols = Lists.newArrayList();
        for (String col : modelDesc.getMetrics()) {
            TblColRef colRef = modelDesc.findColumn(col);
            if (colRef != null) {
                measureCols.add(colRef);
            }
        }
        measureCols.addAll(dimensionCols);

        List<FunctionDesc> measureFuncs = Lists.newArrayList();
        for (TblColRef colRef : measureCols) {
            // Distinct Count
            measureFuncs.add(CubeDescUtil.newFunctionDesc(modelDesc, FunctionDesc.FUNC_COUNT_DISTINCT,
                    ParameterDesc.newInstance(colRef), "hllc(10)"));
            if (colRef.getType().isNumberFamily()) {
                // SUM
                measureFuncs.add(CubeDescUtil.newFunctionDesc(modelDesc, FunctionDesc.FUNC_SUM,
                        ParameterDesc.newInstance(colRef), colRef.getDatatype()));
                // MAX
                measureFuncs.add(CubeDescUtil.newFunctionDesc(modelDesc, FunctionDesc.FUNC_MAX,
                        ParameterDesc.newInstance(colRef), colRef.getDatatype()));
                // MIN
                measureFuncs.add(CubeDescUtil.newFunctionDesc(modelDesc, FunctionDesc.FUNC_MIN,
                        ParameterDesc.newInstance(colRef), colRef.getDatatype()));

                // PERCENTILE
                measureFuncs.add(CubeDescUtil.newFunctionDesc(modelDesc, PercentileMeasureType.FUNC_PERCENTILE,
                        ParameterDesc.newInstance(colRef), colRef.getDatatype()));
            }
        }

        // Build analytics domain
        return new Domain(modelDesc, dimensionCols, measureFuncs);
    }
}
