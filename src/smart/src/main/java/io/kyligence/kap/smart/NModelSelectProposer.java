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

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;

public class NModelSelectProposer extends NAbstractProposer {

    private final NDataflowManager dataflowManager;

    NModelSelectProposer(NSmartContext smartContext) {
        super(smartContext);

        dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        if (CollectionUtils.isEmpty(modelContexts))
            return;

        Set<String> selectedModel = Sets.newHashSet();
        for (NSmartContext.NModelContext modelContext : modelContexts) {
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = compareWithFactTable(modelTree);
            if (model == null || selectedModel.contains(model.getUuid())) {
                // original model is allowed to be selected one context in batch
                // to avoid modification conflict
                continue;
            }
            // found matched, then use it
            modelContext.setOrigModel(model);
            selectedModel.add(model.getUuid());
            NDataModel targetModel = NDataModel.getCopyOf(model);
            initModel(targetModel);
            targetModel.getComputedColumnDescs()
                    .forEach(cc -> smartContext.getUsedCC().put(cc.getExpression(), cc));
            modelContext.setTargetModel(targetModel);
        }

        // if manual maintain type and selected model is null, record error message
        final ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);

        if (projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            smartContext.getModelContexts().forEach(modelCtx -> {
                if (modelCtx.withoutTargetModel()) {
                    modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                        AccelerateInfo accelerateInfo = accelerateInfoMap.get(olapContext.sql);
                        accelerateInfo.setBlockingCause(
                                new IllegalStateException("No model matches this query. In the model designer project, "
                                        + "the system is not allowed to suggest a new model accelerate this query."));
                    });
                }
            });
        }
    }

    private void initModel(NDataModel modelDesc) {
        final NTableMetadataManager manager = NTableMetadataManager.getInstance(kylinConfig, project);
        modelDesc.init(kylinConfig, manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    private NDataModel compareWithFactTable(ModelTree modelTree) {
        for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
            if (matchModelTree(model, modelTree)) {
                return model;
            }
        }
        return null;
    }

    public static boolean matchModelTree(NDataModel model, ModelTree modelTree) {
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
            return model.getJoinsGraph().match(joinsGraph, Maps.newHashMap()) ||
                    joinsGraph.match(model.getJoinsGraph(), Maps.newHashMap());
        }
        return false;
    }
}
