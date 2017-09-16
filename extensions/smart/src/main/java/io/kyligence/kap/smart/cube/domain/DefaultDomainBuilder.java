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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.util.CubeDescUtil;

public class DefaultDomainBuilder implements IDomainBuilder {
    private final QueryStats queryStats;
    private final CubeDesc origCubeDesc;
    private final SmartConfig config;

    public DefaultDomainBuilder(SmartConfig config, QueryStats queryStats, CubeDesc origCubeDesc) {
        this.queryStats = queryStats;
        this.origCubeDesc = origCubeDesc;
        this.config = config;
    }

    @Override
    public Domain build() {
        Set<TblColRef> dimensionCols = buildDimensions();
        List<FunctionDesc> measures = buildMeasures();

        return new Domain(origCubeDesc.getModel(), dimensionCols, measures);
    }

    private Set<TblColRef> buildDimensions() {
        boolean queryEnabled = config.getDomainQueryEnabled();
        Set<TblColRef> dimensionCols = Sets.newHashSet();
        RowKeyColDesc[] rowKeyCols = origCubeDesc.getRowkey().getRowKeyColumns();

        if (queryEnabled && queryStats != null) {
            DataModelDesc modelDesc = origCubeDesc.getModel();

            Set<String> appeared = queryStats.getAppears().keySet();
            for (String appear : appeared) {
                dimensionCols.add(modelDesc.findColumn(appear));
            }
        } else {
            for (int i = 0; i < rowKeyCols.length; i++) {
                dimensionCols.add(rowKeyCols[rowKeyCols.length - i - 1].getColRef());
            }
        }

        return dimensionCols;
    }

    private List<FunctionDesc> buildMeasures() {
        boolean queryEnabled = config.getMeasureQueryEnabled();
        DataModelDesc modelDesc = origCubeDesc.getModel();
        List<FunctionDesc> measures = Lists.newArrayList();
        if (queryStats != null && queryStats.getMeasures() != null) {
            measures.addAll(queryStats.getMeasures());
        }

        if (!queryEnabled) {
            List<TblColRef> measureCols = new ArrayList<>();
            for (String col : modelDesc.getMetrics()) {
                TblColRef colRef = modelDesc.findColumn(col);
                if (colRef != null) {
                    measureCols.add(colRef);
                }
            }

            for (TblColRef colRef : measureCols) {
                if (colRef.getType().isNumberFamily()) {
                    // SUM
                    measures.add(CubeDescUtil.newFunctionDesc(modelDesc, "SUM", ParameterDesc.newInstance(colRef),
                            colRef.getDatatype()));
                }
            }
        }
        return measures;
    }
}
