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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinsGraph;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.ComputedColumnRewriter;
import io.kyligence.kap.query.util.QueryAliasMatchInfo;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractContext.ModelContext;
import io.kyligence.kap.smart.ModelOptProposer;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerBuilder;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelMaster {

    private final AbstractContext.ModelContext modelContext;
    private final ProposerProvider proposerProvider;
    private final String project;

    public ModelMaster(AbstractContext.ModelContext modelContext) {
        this.modelContext = modelContext;
        this.proposerProvider = ProposerProvider.create(modelContext);
        this.project = modelContext.getProposeContext().getProject();
    }

    NDataModel proposeInitialModel() {
        NDataModel dataModel = new NDataModel();
        dataModel.setRootFactTableName(modelContext.getModelTree().getRootFactTable().getIdentity());
        dataModel.setDescription(StringUtils.EMPTY);
        dataModel.setFilterCondition(StringUtils.EMPTY);
        dataModel.setComputedColumnDescs(Lists.newArrayList());

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(dataModel);
        NDataModel.Measure countStarMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", NDataModel.MEASURE_ID_BASE);
        dataModel.setAllMeasures(Lists.newArrayList(countStarMeasure));
        log.info("Initialized a new model({}) for no compatible one to use.", dataModel.getId());
        return dataModel;
    }

    public NDataModel proposeJoins(NDataModel dataModel) {
        if (modelContext.getProposeContext() instanceof ModelReuseContextOfSemiV2) {
            ModelReuseContextOfSemiV2 context = (ModelReuseContextOfSemiV2) modelContext.getProposeContext();
            if (!context.isCanCreateNewModel()) {
                Preconditions.checkState(dataModel != null, ModelOptProposer.NO_COMPATIBLE_MODEL_MSG);
                return dataModel;
            }
        }

        log.info("Start proposing join relations.");
        if (dataModel == null) {
            dataModel = proposeInitialModel();
        }
        dataModel = proposerProvider.getJoinProposer().propose(dataModel);
        log.info("Proposing join relations completed successfully.");
        return dataModel;
    }

    public NDataModel proposeScope(NDataModel dataModel) {
        log.info("Start proposing dimensions and measures.");
        dataModel = proposerProvider.getScopeProposer().propose(dataModel);
        log.info("Proposing dimensions and measures completed successfully.");
        return dataModel;
    }

    public NDataModel proposePartition(NDataModel dataModel) {
        log.info("Start proposing partition column.");
        dataModel = proposerProvider.getPartitionProposer().propose(dataModel);
        log.info("Proposing partition column completed successfully.");
        return dataModel;
    }

    public NDataModel proposeComputedColumn(NDataModel dataModel) {
        log.info("Start proposing computed columns.");
        initExcludedLookChecker(modelContext, dataModel);
        boolean turnOnCC = modelContext.getProposeContext().getKapConfig().isImplicitComputedColumnConvertEnabled();
        if (!turnOnCC) {
            log.warn("The feature of proposing computed column in Kyligence Enterprise has been turned off.");
            if (modelContext.getChecker() == null) {
                Set<String> excludedTables = modelContext.getProposeContext().getExtraMeta().getExcludedTables();
                ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, dataModel.getJoinTables(),
                        dataModel);
                modelContext.setChecker(checker);
            }
            return dataModel;
        }

        List<ComputedColumnDesc> originalCCs = Lists.newArrayList(dataModel.getComputedColumnDescs());
        try {
            dataModel = proposerProvider.getComputedColumnProposer().propose(dataModel);
            if (modelContext.isNeedUpdateCC()) {
                // New CC detected, need to rebuild ModelContext regarding new coming CC
                log.info("Start using proposed computed columns to update the model({})", dataModel.getId());
                updateContextWithCC(dataModel);
            }
            log.info("Proposing computed column completed successfully.");
        } catch (Exception e) {
            log.error("Propose failed, will discard new computed columns.", e);
            dataModel.setComputedColumnDescs(originalCCs);
        }
        return dataModel;
    }

    private void initExcludedLookChecker(ModelContext modelContext, NDataModel dataModel) {
        if (modelContext.getChecker() == null) {
            Set<String> excludedTables = modelContext.getProposeContext().getExtraMeta().getExcludedTables();
            ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, dataModel.getJoinTables(),
                    dataModel);
            modelContext.setChecker(checker);
        }
    }

    public NDataModel shrinkComputedColumn(NDataModel dataModel) {
        return proposerProvider.getShrinkComputedColumnProposer().propose(dataModel);
    }

    private void updateContextWithCC(NDataModel dataModel) {
        List<String> originQueryList = Lists.newArrayList();
        modelContext.getModelTree().getOlapContexts().stream() //
                .filter(context -> !StringUtils.isEmpty(context.sql)) //
                .forEach(context -> originQueryList.add(context.sql));
        if (originQueryList.isEmpty()) {
            log.warn("Failed to replace cc expression in original sql with proposed computed columns, "
                    + "early termination of the method of updateContextWithCC");
            return;
        }

        // Rebuild modelTrees and find match one to replace original
        KylinConfig kylinConfig = modelContext.getProposeContext().getSmartConfig().getKylinConfig();
        try (AbstractQueryRunner extractor = new QueryRunnerBuilder(project, kylinConfig,
                originQueryList.toArray(new String[0])).of(Lists.newArrayList(dataModel)).build()) {
            log.info("Start to rebuild modelTrees after replace cc expression with cc name.");
            extractor.execute();
            final AbstractContext proposeContext = modelContext.getProposeContext();
            List<ModelTree> modelTrees = new GreedyModelTreesBuilder(kylinConfig, project, proposeContext) //
                    .build(extractor.filterNonModelViewOlapContexts(), null);
            ModelTree updatedModelTree = null;
            for (ModelTree modelTree : modelTrees) {
                boolean match = proposeContext instanceof SmartContext //
                        ? modelTree.hasSameSubGraph(dataModel)
                        : modelTree.isExactlyMatch(dataModel, proposeContext.isPartialMatch(), proposeContext.isPartialMatchNonEqui());
                if (match) {
                    updatedModelTree = modelTree;
                    break;
                }
            }
            if (updatedModelTree == null) {
                return;
            }

            // Update context info
            this.modelContext.setModelTree(updatedModelTree);
            updateOlapCtxWithCC(updatedModelTree, dataModel);
            log.info("Rebuild modelTree successfully.");
        } catch (Exception e) {
            log.warn("NModelMaster.updateContextWithCC failed to update model tree", e);
        }
    }

    private void updateOlapCtxWithCC(ModelTree modelTree, NDataModel model) {
        modelTree.getOlapContexts().forEach(context -> {
            JoinsGraph joinsGraph = context.getJoinsGraph() == null
                    ? new JoinsGraph(context.firstTableScan.getTableRef(), context.joins)
                    : context.getJoinsGraph();
            Map<String, String> matches = joinsGraph.matchAlias(model.getJoinsGraph(), false);
            if (matches == null || matches.isEmpty()) {
                return;
            }
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.putAll(matches);
            ComputedColumnRewriter.rewriteCcInnerCol(context, model, new QueryAliasMatchInfo(aliasMapping, null));
        });
    }
}
