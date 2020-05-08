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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NModelSelectProposer extends NAbstractProposer {

    public static final String NO_MODEL_MATCH_PENDING_MSG = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
    public static final String CC_ACROSS_MODELS_PENDING_MSG = "No model matches the SQL. Please add a model that contains all the computed columns used in the query.";
    private final NDataModelManager dataModelManager;

    public NModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
    }

    @Override
    public void execute() {
        List<AbstractContext.NModelContext> modelContexts = proposeContext.getModelContexts();
        if (CollectionUtils.isEmpty(modelContexts)) {
            log.warn("Something wrong happened in the preceding step of sql analysis. "
                    + "Cannot continue auto-modeling without modelTrees.");
            return;
        }

        Set<String> selectedModel = Sets.newHashSet();
        for (AbstractContext.NModelContext modelContext : modelContexts) {
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = selectExistedModel(modelTree, modelContext);
            if (model == null || (selectedModel.contains(model.getUuid()) && !modelContext.isSnapshotSelected())) {
                // original model is allowed to be selected one context in batch
                // to avoid modification conflict
                if (model != null) {
                    log.info("An existing model({}) compatible to more than one modelTree, "
                            + "in order to avoid modification conflict, ignore this match.", model.getId());
                }
                continue;
            }
            // found matched, then use it
            modelContext.setOriginModel(model);
            NDataModel targetModel = dataModelManager.copyBySerialization(model);
            initModel(targetModel);
            targetModel.getComputedColumnDescs().forEach(cc -> {
                modelContext.getUsedCC().put(cc.getExpression(), cc);
            });
            modelContext.setTargetModel(targetModel);

            if (!modelContext.isSnapshotSelected()) {
                selectedModel.add(model.getUuid());
            }
        }

        proposeContext.handleExceptionAfterModelSelect();
    }

    private void initModel(NDataModel modelDesc) {
        final NTableMetadataManager manager = NTableMetadataManager.getInstance(kylinConfig, project);
        modelDesc.init(kylinConfig, manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    private NDataModel selectExistedModel(ModelTree modelTree, AbstractContext.NModelContext modelContext) {
        List<NDataModel> originModels = proposeContext.getOriginModels();
        for (NDataModel model : originModels) {
            List<OLAPContext> retainedOLAPContexts = retainCapableOLAPContexts(model,
                    Lists.newArrayList(modelTree.getOlapContexts()));
            if (retainedOLAPContexts.isEmpty()) {
                continue;
            }

            boolean match = proposeContext instanceof NSmartContext //
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

            if (!(proposeContext instanceof NSmartContext) && matchSnapshot(model, modelTree)) {
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

    public static boolean matchSnapshot(NDataModel model, ModelTree modelTree) {
        if (!modelTree.getJoins().isEmpty() || modelTree.getRootFactTable().isIncrementLoading())
            return false;

        val modelTreeRootTable = modelTree.getRootFactTable().getIdentity();
        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .getDataflow(model.getUuid());

        if (dataflow == null || !dataflow.isReady() || dataflow.getLatestReadySegment() == null)
            return false;

        return dataflow.getLatestReadySegment().getSnapshots().containsKey(modelTreeRootTable);
    }

    @Override
    public String getIdentifierName() {
        return "ModelSelectProposer";
    }
}
