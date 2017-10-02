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

package io.kyligence.kap.smart.model.proposer;

import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.DimensionAdvisor;
import io.kyligence.kap.smart.model.ModelContext;

/**
 * Define dimension and measure according to datatype, which is the default logic
 * on KAP 2.4
 */
public class DefaultScopeProposer extends AbstractModelProposer {
    private final DimensionAdvisor dimensionAdvisor;

    public DefaultScopeProposer(ModelContext modelCtx) {
        super(modelCtx);
        dimensionAdvisor = new DimensionAdvisor(modelContext.getKylinConfig());
    }

    @Override
    protected void doPropose(DataModelDesc modelDesc) {
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(modelContext.getKylinConfig());
        List<ModelDimensionDesc> dims = Lists.newArrayList();
        List<String> metrics = Lists.newArrayList();

        TableDesc rootFactTbl = metadataManager.getTableDesc(modelDesc.getRootFactTableName(),
                modelContext.getProject());
        collectDimensionAndMeasure(rootFactTbl, rootFactTbl.getName(), dims, metrics);

        for (JoinTableDesc joinTblDesc : modelDesc.getJoinTables()) {
            TableDesc tblDesc = metadataManager.getTableDesc(joinTblDesc.getTable(), modelContext.getProject());
            collectDimensionAndMeasure(tblDesc, joinTblDesc.getAlias(), dims, metrics);
        }

        modelDesc.setDimensions(dims);
        modelDesc.setMetrics(metrics.toArray(new String[0]));
    }

    private void collectDimensionAndMeasure(TableDesc tableDesc, String alias, List<ModelDimensionDesc> dimCollector,
            List<String> metricCollector) {
        Map<String, DimensionAdvisor.ColumnSuggestionType> tblAdv = dimensionAdvisor
                .inferDimensionSuggestions(tableDesc.getIdentity(), modelContext.getProject());
        List<String> tblDims = Lists.newArrayList();
        for (Map.Entry<String, DimensionAdvisor.ColumnSuggestionType> colAdv : tblAdv.entrySet()) {
            if (colAdv.getValue() == DimensionAdvisor.ColumnSuggestionType.METRIC) {
                metricCollector.add(alias + "." + colAdv.getKey());
            } else {
                tblDims.add(colAdv.getKey());
            }
        }

        ModelDimensionDesc dimensionDesc = new ModelDimensionDesc();
        dimensionDesc.setTable(alias);
        dimensionDesc.setColumns(tblDims.toArray(new String[0]));
        dimCollector.add(dimensionDesc);
    }
}
