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

package io.kyligence.kap.smart;

import static java.util.stream.Collectors.groupingBy;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSelectProposer extends AbstractProposer {

    public static final String NO_MODEL_MATCH_PENDING_MSG = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
    public static final String CC_ACROSS_MODELS_PENDING_MSG = "No model matches the SQL. Please add a model that contains all the computed columns used in the query.";
    private final NDataModelManager dataModelManager;

    public ModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public void execute() {
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        if (CollectionUtils.isEmpty(modelContexts)) {
            log.warn("Something wrong happened in the preceding step of sql analysis. "
                    + "Cannot continue auto-modeling without modelTrees.");
            return;
        }

        val allSubModelContexts = Lists.<AbstractContext.ModelContext> newArrayList();
        Map<String, AbstractContext.ModelContext> selectedModel = Maps.newHashMap();
        selectModelForModelContext(modelContexts, allSubModelContexts, selectedModel);
        if (CollectionUtils.isNotEmpty(allSubModelContexts)) {
            selectModelForModelContext(allSubModelContexts, Lists.newArrayList(), selectedModel);
        }

        proposeContext.handleExceptionAfterModelSelect();
    }

    private void selectModelForModelContext(List<AbstractContext.ModelContext> modelContexts,
            List<AbstractContext.ModelContext> allSubModelContexts,
            Map<String, AbstractContext.ModelContext> selectedModel) {
        val modelContextIterator = modelContexts.listIterator();
        Set<AbstractContext.ModelContext> mergedModelContexts = Sets.newHashSet();
        while (modelContextIterator.hasNext()) {
            val modelContext = modelContextIterator.next();
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = selectExistedModel(modelTree, modelContext);

            if (model == null) {
                if (CollectionUtils.isEmpty(proposeContext.getOriginModels())) {
                    continue;
                }
                val subModelContexts = splitModelContext(modelContext);
                if (subModelContexts.size() > 1) {
                    modelContextIterator.remove();
                    subModelContexts.forEach(modelContextIterator::add);
                    allSubModelContexts.addAll(subModelContexts);
                }
                continue;
            }

            if (selectedModel.containsKey(model.getUuid()) && !modelContext.isSnapshotSelected()) {
                if (KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()) {
                    AbstractContext.ModelContext anotherModelContext = selectedModel.get(model.getUuid());
                    AbstractContext.ModelContext newModelContext = new GreedyModelTreesBuilder.TreeBuilder(
                            model.getRootFactTable().getTableDesc(),
                            NTableMetadataManager.getInstance(dataModelManager.getConfig(), project).getAllTablesMap(),
                            proposeContext).mergeModelContext(proposeContext, modelContext, anotherModelContext);
                    setModelContextModel(newModelContext, model);
                    mergedModelContexts.add(modelContext);
                    mergedModelContexts.add(anotherModelContext);
                    modelContextIterator.add(newModelContext);
                    selectedModel.put(model.getUuid(), newModelContext);
                } else {
                    // original model is allowed to be selected one context in batch
                    // to avoid modification conflict
                    log.info("An existing model({}) compatible to more than one modelTree, "
                            + "in order to avoid modification conflict, ignore this match.", model.getId());
                }
                continue;
            }
            // found matched, then use it
            setModelContextModel(modelContext, model);
            if (!modelContext.isSnapshotSelected()) {
                selectedModel.put(model.getUuid(), modelContext);
            }
        }
        modelContexts.removeAll(mergedModelContexts);
    }

    private void setModelContextModel(AbstractContext.ModelContext modelContext, NDataModel model) {
        modelContext.setOriginModel(model);
        NDataModel targetModel = dataModelManager.copyBySerialization(model);
        initModel(targetModel);
        targetModel.getComputedColumnDescs().forEach(cc -> {
            modelContext.getUsedCC().put(cc.getExpression(), cc);
        });
        modelContext.setTargetModel(targetModel);
    }

    private List<AbstractContext.ModelContext> splitModelContext(AbstractContext.ModelContext modelContext) {
        val sqlOLAPContextMap = modelContext.getModelTree().getOlapContexts().stream()
                .collect(groupingBy(olapContext -> olapContext.sql));
        if (sqlOLAPContextMap.size() == 1) {
            return Lists.newArrayList(modelContext);
        }

        val subModelContexts = Lists.<AbstractContext.ModelContext> newArrayList();
        val sqlModelContextMap = Maps.<String, AbstractContext.ModelContext> newHashMap();
        val nonSelectedSqls = Lists.<String> newArrayList();
        val nonSelectedOlapContexts = Lists.<Collection<OLAPContext>> newArrayList();
        val sqlSelectedModelMap = Maps.<NDataModel, List<String>> newHashMap();
        for (Map.Entry<String, List<OLAPContext>> entry : sqlOLAPContextMap.entrySet()) {
            val sql = entry.getKey();
            val olapContexts = entry.getValue();
            val sqlModelContext = buildModelContext(Lists.newArrayList(sql),
                    Lists.<Collection<OLAPContext>> newArrayList(olapContexts)).get(0);
            val selectedModel = selectExistedModel(sqlModelContext.getModelTree(), sqlModelContext);
            if (selectedModel == null) {
                nonSelectedSqls.add(sql);
                nonSelectedOlapContexts.add(olapContexts);
            } else {
                sqlSelectedModelMap.putIfAbsent(selectedModel, Lists.newArrayList());
                sqlSelectedModelMap.get(selectedModel).add(sql);
                sqlModelContextMap.put(sql, sqlModelContext);
            }
        }

        // merge non-selected model contexts
        subModelContexts.addAll(buildModelContext(nonSelectedSqls, nonSelectedOlapContexts));

        // merge selected model contexts by model id
        sqlSelectedModelMap.forEach((model, sqls) -> {
            if (sqls.size() == 1) {
                subModelContexts.add(sqlModelContextMap.get(sqls.get(0)));
            } else {
                val olapContexts = Lists.<Collection<OLAPContext>> newArrayList();
                for (String sql : sqls) {
                    olapContexts.add(sqlOLAPContextMap.get(sql));
                }
                subModelContexts.addAll(buildModelContext(sqls, olapContexts));
            }
        });

        return subModelContexts;
    }

    private List<AbstractContext.ModelContext> buildModelContext(List<String> sqls,
            List<Collection<OLAPContext>> olapContexts) {
        return new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext) //
                .build(sqls, olapContexts, null) //
                .stream() //
                .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()) //
                .map(proposeContext::createModelContext) //
                .collect(Collectors.toList());
    }

    private void initModel(NDataModel modelDesc) {
        NTableMetadataManager manager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelDesc.init(KylinConfig.getInstanceFromEnv(), manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    private NDataModel selectExistedModel(ModelTree modelTree, AbstractContext.ModelContext modelContext) {
        List<NDataModel> originModels = proposeContext.getOriginModels();
        for (NDataModel model : originModels) {
            List<OLAPContext> retainedOLAPContexts = retainCapableOLAPContexts(model,
                    Lists.newArrayList(modelTree.getOlapContexts()));
            if (retainedOLAPContexts.isEmpty()) {
                continue;
            }

            boolean match = proposeContext instanceof SmartContext //
                    ? modelTree.hasSameSubGraph(model)
                    : modelTree.isExactlyMatch(model, proposeContext.isPartialMatch());

            if (match) {
                List<OLAPContext> disabledList = modelTree.getOlapContexts().stream()
                        .filter(context -> !retainedOLAPContexts.contains(context)).collect(Collectors.toList());
                disabledList.forEach(context -> {
                    AccelerateInfo accelerateInfo = new AccelerateInfo();
                    accelerateInfo.setPendingMsg(CC_ACROSS_MODELS_PENDING_MSG);
                    proposeContext.getAccelerateInfoMap().put(context.sql, accelerateInfo);
                });
                modelTree.getOlapContexts().clear();
                modelTree.getOlapContexts().addAll(retainedOLAPContexts);
                modelContext.setSnapshotSelected(false);
                return model;
            }

            val project = modelContext.getProposeContext().getProject();
            val config = modelContext.getProposeContext().getSmartConfig().getKylinConfig();
            if (!(proposeContext instanceof SmartContext) && matchSnapshot(config, project, modelTree)) {
                modelContext.setSnapshotSelected(true);
                return model;
            }
        }
        return null;
    }

    private List<OLAPContext> retainCapableOLAPContexts(NDataModel model, List<OLAPContext> olapContexts) {
        NDataModel targetModel = dataModelManager.copyForWrite(model);
        initModel(targetModel);
        Set<ColumnDesc> ccColDesc = filterTblColRefOfCC(targetModel.getEffectiveCols().values());
        Iterator<OLAPContext> iterator = olapContexts.iterator();
        while (iterator.hasNext()) {
            OLAPContext context = iterator.next();
            Set<ColumnDesc> ccColDescInCtx = filterTblColRefOfCC(context.allColumns);
            if (!ccColDesc.containsAll(ccColDescInCtx)) {
                iterator.remove();
            }
        }
        return olapContexts;
    }

    private Set<ColumnDesc> filterTblColRefOfCC(Set<TblColRef> tableColRefSet) {
        Preconditions.checkArgument(tableColRefSet != null);
        return tableColRefSet.stream() //
                .filter(tblColRef -> tblColRef.getColumnDesc().isComputedColumn()) //
                .map(TblColRef::getColumnDesc).collect(Collectors.toSet());
    }

    public static boolean matchSnapshot(KylinConfig config, String project, ModelTree modelTree) {
        if (!modelTree.getJoins().isEmpty() || modelTree.getRootFactTable().isIncrementLoading())
            return false;

        val modelTreeRootTable = modelTree.getRootFactTable().getIdentity();

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
        return StringUtils.isNotEmpty(tableManager.getTableDesc(modelTreeRootTable).getLastSnapshotPath());
    }

    @Override
    public String getIdentifierName() {
        return "ModelSelectProposer";
    }
}
