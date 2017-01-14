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

package io.kyligence.kap.modeling.auto.proposer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.modeling.auto.mockup.MockupCubeBuilder;

public class ModelProposer extends AbstractProposer {

    private DataModelDesc model;

    public ModelProposer(DataModelDesc model) {
        this.model = model;
    }

    public void preCheck() {
        if (getDomain() == null) {
            throw new IllegalStateException("Analytics Domain is not ready for auto cubing.");
        }
    }

    @Override
    public CubeDesc propose() {
        preCheck();
        return new MockupCubeBuilder(getDomain()).build();
    }

    public AnalyticsDomain getDomain() {
        if (model == null || model.getAllTables() == null) {
            return null;
        }

        // Prepare columns info
        Set<TblColRef> primaryKeys = new HashSet<>();
        Set<TblColRef> foreignKeys = new HashSet<>();
        for (JoinTableDesc fTable : model.getJoinTables()) {
            primaryKeys.addAll(Arrays.asList(fTable.getJoin().getPrimaryKeyColumns()));
            foreignKeys.addAll(Arrays.asList(fTable.getJoin().getForeignKeyColumns()));
        }

        // Setup dimensions
        List<TblColRef> dimensionCols = new ArrayList<>();
        for (ModelDimensionDesc dim : model.getDimensions()) {
            TableRef lookupTable = model.findTable(dim.getTable());
            if (lookupTable == null) {
                continue;
            }
            for (String col : dim.getColumns()) {
                TblColRef colRef = lookupTable.getColumn(col);
                if (primaryKeys.contains(colRef) || foreignKeys.contains(colRef)) {
                    // Skip, add it later
                    continue;
                }
                if (colRef != null) {
                    dimensionCols.add(colRef);
                }
            }
        }
        for (TblColRef foreignKey : foreignKeys) {
            dimensionCols.add(foreignKey);
        }

        // Setup measures
        List<TblColRef> measureCols = new ArrayList<>();
        for (String col : model.getMetrics()) {
            TblColRef colRef = model.findColumn(col);
            if (colRef != null) {
                measureCols.add(colRef);
                colRef.getType().isNumberFamily();
            }
        }
        Set<FunctionDesc> measureFuncs = new HashSet<>();
        for (TblColRef colRef : measureCols) {
            // Distinct Count
            measureFuncs.add(FunctionDesc.newInstance("COUNT_DISTINCT", ParameterDesc.newInstance(colRef), "hllc(10)"));
            if (colRef.getType().isNumberFamily()) {
                // SUM
                measureFuncs.add(FunctionDesc.newInstance("SUM", ParameterDesc.newInstance(colRef), colRef.getDatatype()));
                // MAX
                measureFuncs.add(FunctionDesc.newInstance("MAX", ParameterDesc.newInstance(colRef), colRef.getDatatype()));
                // MIN
                measureFuncs.add(FunctionDesc.newInstance("MIN", ParameterDesc.newInstance(colRef), colRef.getDatatype()));
            }
        }

        // Build analytics domain
        AnalyticsDomain domain = new AnalyticsDomain();
        domain.setModel(model);
        domain.setDimensionCols(dimensionCols);
        domain.setMeasureFuncs(measureFuncs);
        return domain;
    }
}
