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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;

public class NModelSelectProposer extends NAbstractProposer {

    private static final String EXCEPTION_MSG = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
    private final NDataflowManager dataflowManager;
    private final NDataModelManager dataModelManager;

    NModelSelectProposer(NSmartContext smartContext) {
        super(smartContext);

        dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        if (CollectionUtils.isEmpty(modelContexts)) {
            logger.warn("Something wrong happened in the preceding step of sql analysis. "
                    + "Cannot continue auto-modeling without modelTrees.");
            return;
        }

        Set<String> selectedModel = Sets.newHashSet();
        for (NSmartContext.NModelContext modelContext : modelContexts) {
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = selectExistedModel(modelTree, modelContext);
            if (model == null || selectedModel.contains(model.getUuid())) {
                // original model is allowed to be selected one context in batch
                // to avoid modification conflict
                if (model != null) {
                    logger.info("An existing model({}) compatible to more than one modelTree, "
                            + "in order to avoid modification conflict, ignore this match.", model.getId());
                }
                continue;
            }
            // found matched, then use it
            modelContext.setOriginModel(model);
            selectedModel.add(model.getUuid());
            NDataModel targetModel = dataModelManager.copyForWrite(model);
            initModel(targetModel);
            targetModel.getComputedColumnDescs().forEach(cc -> {
                modelContext.getUsedCC().put(cc.getExpression(), cc);
            });
            modelContext.setTargetModel(targetModel);
        }

        //if cannot create new model, record pending message
        if (!smartContext.isCouldCreateNewModel()) {
            smartContext.getModelContexts().forEach(modelCtx -> {
                if (modelCtx.withoutTargetModel()) {
                    modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                        AccelerateInfo accelerateInfo = smartContext.getAccelerateInfoMap().get(olapContext.sql);
                        accelerateInfo.setPendingMsg(EXCEPTION_MSG);
                    });
                }
            });
        }
    }

    private void initModel(NDataModel modelDesc) {
        final NTableMetadataManager manager = NTableMetadataManager.getInstance(kylinConfig, project);
        modelDesc.init(kylinConfig, manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    private NDataModel selectExistedModel(ModelTree modelTree, NSmartContext.NModelContext modelContext) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(this.smartContext.getProject());
        List<NDataModel> originModels = getOriginModels();
        for (NDataModel model : originModels) {
            if (!isContainAllCcColsInModel(model, modelTree))
                continue;

            if (matchModelTree(model, modelTree, projectInstance.isSmartMode())) {
                modelContext.setSnapshotSelected(false);
                return model;
            }

            if (!projectInstance.isSmartMode() && matchSnapshot(model, modelTree)) {
                modelContext.setSnapshotSelected(true);
                return model;
            }
        }
        return null;
    }

    private boolean isContainAllCcColsInModel(NDataModel model, ModelTree modelTree) {
        NDataModel targetModel = dataModelManager.copyForWrite(model);
        initModel(targetModel);
        Set<ColumnDesc> ccColDesc = targetModel.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().isComputedColumn()).map(TblColRef::getColumnDesc)
                .collect(Collectors.toSet());
        for (OLAPContext context : modelTree.getOlapContexts()) {
            Set<ColumnDesc> ccColDescInCtx = context.allColumns.stream()
                    .filter(tblColRef -> tblColRef.getColumnDesc().isComputedColumn()).map(TblColRef::getColumnDesc)
                    .collect(Collectors.toSet());
            if (!ccColDesc.containsAll(ccColDescInCtx)) {
                return false;
            }
        }
        return true;
    }

    public static boolean matchModelTree(NDataModel model, ModelTree modelTree, boolean couldModifyExistedModel) {
        if (model.getRootFactTable().getTableIdentity().equals(modelTree.getRootFactTable().getIdentity())) {
            List<JoinDesc> modelTreeJoins = Lists.newArrayListWithExpectedSize(modelTree.getJoins().size());
            TableRef factTblRef = null;
            if (modelTree.getJoins().isEmpty()) {
                factTblRef = model.getRootFactTable();
            } else {
                Map<TableRef, TableRef> joinMap = Maps.newHashMap();
                modelTree.getJoins().values().forEach(joinTableDesc -> {
                    modelTreeJoins.add(joinTableDesc.getJoin());
                    joinMap.put(joinTableDesc.getJoin().getPKSide(), joinTableDesc.getJoin().getFKSide());
                });

                for (Map.Entry<TableRef, TableRef> joinEntry : joinMap.entrySet()) {
                    if (!joinMap.containsKey(joinEntry.getValue())) {
                        factTblRef = joinEntry.getValue();
                        break;
                    }
                }
            }
            JoinsGraph joinsGraph = new JoinsGraph(factTblRef, modelTreeJoins);

            if (couldModifyExistedModel) {
                return model.getJoinsGraph().match(joinsGraph, Maps.newHashMap())
                        || joinsGraph.match(model.getJoinsGraph(), Maps.newHashMap());
            } else {
                return joinsGraph.match(model.getJoinsGraph(), Maps.newHashMap(),
                        KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel());
            }
        }

        return false;
    }

    public static boolean matchSnapshot(NDataModel model, ModelTree modelTree) {
        if (!modelTree.getJoins().isEmpty() || modelTree.getRootFactTable().isIncrementLoading())
            return false;

        val modelTreeRootTable = modelTree.getRootFactTable().getIdentity();
        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject()).getDataflow(model.getUuid());

        if (dataflow == null || !dataflow.isReady() || dataflow.getLatestReadySegment() == null)
            return false;

        return dataflow.getLatestReadySegment().getSnapshots().containsKey(modelTreeRootTable);
    }
}
