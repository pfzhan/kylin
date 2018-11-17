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

package io.kyligence.kap.smart.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.ConvertToComputedColumn;
import io.kyligence.kap.smart.NModelSelectProposer;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.NQueryRunnerFactory;
import io.kyligence.kap.smart.util.CubeUtils;

public class NModelMaster {

    private static final Logger LOGGER = LoggerFactory.getLogger(NModelMaster.class);

    private NSmartContext.NModelContext context;
    private NProposerProvider proposerProvider;

    public NModelMaster(NSmartContext.NModelContext ctx) {
        this.context = ctx;
        this.proposerProvider = NProposerProvider.create(this.context);
    }

    public NSmartContext.NModelContext getContext() {
        return context;
    }

    public NDataModel proposeInitialModel() {
        NDataModel modelDesc = new NDataModel();
        modelDesc.updateRandomUuid();
        modelDesc.setName(modelDesc.getUuid());
        modelDesc.setRootFactTableName(context.getModelTree().getRootFactTable().getIdentity());
        modelDesc.setDescription(StringUtils.EMPTY);
        modelDesc.setFilterCondition(StringUtils.EMPTY);
        modelDesc.setPartitionDesc(new PartitionDesc());
        modelDesc.setComputedColumnDescs(new ArrayList<>());

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(modelDesc);
        NDataModel.Measure countStarMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", NDataModel.MEASURE_ID_BASE);
        modelDesc.setAllMeasures(Lists.newArrayList(countStarMeasure));
        return modelDesc;
    }

    public NDataModel proposeJoins(NDataModel model) {
        return proposerProvider.getJoinProposer().propose(model);
    }

    public NDataModel proposeScope(NDataModel model) {
        return proposerProvider.getScopeProposer().propose(model);
    }

    public NDataModel proposePartition(NDataModel model) {
        return proposerProvider.getPartitionProposer().propose(model);
    }

    public NDataModel proposeComputedColumn(NDataModel model) {
        int retryMax = KapConfig.wrap(context.getSmartContext().getKylinConfig()).getComputedColumnMaxRecursionTimes();
        int retryCount = 0;
        
        do {
            int cntOriginCC = model.getComputedColumnDescs().size();
            model = proposerProvider.getComputedColumnProposer().propose(model);
            if (model.getComputedColumnDescs().size() == cntOriginCC) {
                break;
            }
            // New CC detected, need to rebuild ModelContext regarding new coming CC
            updateContextWithCC(model);
        } while((retryCount++) < retryMax);
        return model;
    }

    private void updateContextWithCC(NDataModel modelDesc) {
        String project = context.getSmartContext().getProject();
        KylinConfig config = context.getSmartContext().getKylinConfig();
        List<String> sqls = Lists.newArrayList();
        for (OLAPContext olapContext : getContext().getModelTree().getOlapContexts()) {
            String newSql = olapContext.sql;
            if (StringUtils.isEmpty(newSql)) {
                continue;
            }
            try {
                newSql = new ConvertToComputedColumn().transformImpl(newSql, project, modelDesc,
                        modelDesc.getRootFactTable().getTableDesc().getDatabase());
            } catch (Exception e) {
                LOGGER.warn("NModelMaster.updateContextWithCC failed to transform query: {}", newSql, e);
            }
            sqls.add(newSql);
        }

        if (sqls.isEmpty()) {
            return;
        }

        try (AbstractQueryRunner extractor = NQueryRunnerFactory.createForModelSuggestion(config,
                sqls.toArray(new String[0]), 1, project, Lists.newArrayList(modelDesc))) {
            extractor.execute();
            NSmartContext smartContext = context.getSmartContext();
            List<ModelTree> modelTrees = new GreedyModelTreesBuilder(smartContext.getKylinConfig(),
                    smartContext.getProject()).build(sqls, extractor.getAllOLAPContexts(), null);
            ModelTree updatedModelTree = null;
            for (ModelTree modelContext : modelTrees) {
                if (NModelSelectProposer.matchModelTree(modelDesc, modelContext)) {
                    updatedModelTree = modelContext;
                    break;
                }
            }
            if (updatedModelTree == null) {
                return;
            }

            // Update context info
            this.context.setModelTree(updatedModelTree);
        } catch (Exception e) {
            LOGGER.warn("NModelMaster.updateContextWithCC failed to update model tree", e);
        }
    }
}
