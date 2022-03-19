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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.QueryModelPriorities;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.AbstractJoinRule;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSelectProposer extends AbstractProposer {

    public static final String NO_MODEL_MATCH_PENDING_MSG = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
    public static final String CC_ACROSS_MODELS_PENDING_MSG = "No model matches the SQL. Please add a model that contains all the computed columns used in the query.";
    private final NDataModelManager dataModelManager;
    private final AbstractJoinRule joinSelectOptRule;

    public ModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        joinSelectOptRule = AbstractJoinRule.getInstance();
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
        val sqlSelectedModelMap = Maps.<NDataModel, List<String>> newHashMap();

        Map<String, Collection<OLAPContext>> noneSelected = Maps.newHashMap();
        sqlOLAPContextMap.forEach((key, value) -> {
            Map<String, Collection<OLAPContext>> map = Maps.newHashMap();
            map.put(key, value);
            val sqlModelContext = buildModelContext(map).get(0);
            val selectedModel = selectExistedModel(sqlModelContext.getModelTree(), sqlModelContext);
            if (selectedModel == null) {
                noneSelected.put(key, value);
            } else {
                sqlSelectedModelMap.putIfAbsent(selectedModel, Lists.newArrayList());
                sqlSelectedModelMap.get(selectedModel).add(key);
                sqlModelContextMap.put(key, sqlModelContext);
            }
        });

        // merge non-selected model contexts
        subModelContexts.addAll(buildModelContext(noneSelected));

        // merge selected model contexts by model id
        sqlSelectedModelMap.forEach((model, sqls) -> {
            Map<String, Collection<OLAPContext>> map = Maps.newHashMap();
            if (sqls.size() == 1) {
                subModelContexts.add(sqlModelContextMap.get(sqls.get(0)));
            } else {
                for (String sql : sqls) {
                    map.putIfAbsent(sql, Lists.newArrayList());
                    map.get(sql).addAll(sqlOLAPContextMap.get(sql));
                }
                subModelContexts.addAll(buildModelContext(map));
            }
        });

        return subModelContexts;
    }

    private List<AbstractContext.ModelContext> buildModelContext(Map<String, Collection<OLAPContext>> groupedOlapMap) {
        return new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext) //
                .build(groupedOlapMap, null) //
                .stream() //
                .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()) //
                .map(proposeContext::createModelContext) //
                .collect(Collectors.toList());
    }

    private void initModel(NDataModel modelDesc) {
        NTableMetadataManager manager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelDesc.init(KylinConfig.getInstanceFromEnv(), manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    private Comparator<NDataModel> modelSorter(ModelTree modelTree) {
        Map<String, Integer> modelPriorities = Maps.newHashMap();
        if (modelTree != null && modelTree.getOlapContexts() != null && !modelTree.getOlapContexts().isEmpty()) {
            String[] priorities = QueryModelPriorities
                    .getModelPrioritiesFromComment(modelTree.getOlapContexts().iterator().next().sql);
            for (int i = 0; i < priorities.length; i++) {
                modelPriorities.put(priorities[i], i);
            }
        }
        Comparator<NDataModel> sqlHintSorter = Comparator.comparingInt(
                m -> modelPriorities.getOrDefault(m.getAlias().toUpperCase(Locale.ROOT), Integer.MAX_VALUE));

        Comparator<NDataModel> joinSorter = (m1, m2) -> {
            List<JoinTableDesc> joinTables2 = m2.getJoinTables() == null ? Lists.newArrayList() : m2.getJoinTables();
            List<JoinTableDesc> joinTables1 = m1.getJoinTables() == null ? Lists.newArrayList() : m1.getJoinTables();
            List<JoinTableDesc> filteredJoinTables1 = joinTables1.stream()
                    .filter(joinTable -> joinTable.getJoin().isJoinWithFactTable(m1.getRootFactTableName()))
                    .collect(Collectors.toList());
            List<JoinTableDesc> filteredJoinTables2 = joinTables2.stream()
                    .filter(joinTable -> joinTable.getJoin().isJoinWithFactTable(m2.getRootFactTableName()))
                    .collect(Collectors.toList());
            return Integer.compare(filteredJoinTables2.size(), filteredJoinTables1.size());
        };
        Comparator<NDataModel> modifiedSorter = Comparator.comparing(NDataModel::getCreateTime).reversed();
        Comparator<NDataModel> aliasSorter = Comparator.comparing(NDataModel::getAlias).reversed();
        return Ordering.from(sqlHintSorter).compound(joinSorter).compound(modifiedSorter).compound(aliasSorter);
    }

    private NDataModel selectExistedModel(ModelTree modelTree, AbstractContext.ModelContext modelContext) {
        List<NDataModel> originModels = proposeContext.getOriginModels();
        originModels.sort(modelSorter(modelTree));
        for (NDataModel model : originModels) {
            List<OLAPContext> retainedOLAPContexts = retainCapableOLAPContexts(model,
                    Lists.newArrayList(modelTree.getOlapContexts()));
            if (retainedOLAPContexts.isEmpty()) {
                continue;
            }

            boolean match = proposeContext instanceof SmartContext //
                    ? modelTree.hasSameSubGraph(model)
                    : modelTree.isExactlyMatch(model, proposeContext.isPartialMatch(),
                            proposeContext.isPartialMatchNonEqui());

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
            } else if (proposeContext.isCanCreateNewModel() && joinSelectOptRule.isCompatible(model, modelTree)) {
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
                .map(TblColRef::getColumnDesc) //
                .filter(ColumnDesc::isComputedColumn) //
                .collect(Collectors.toSet());
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
