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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.model.ModelTree;

public class NModelSelectProposer extends NAbstractProposer {

    private final NDataModelManager modelManager;

    public NModelSelectProposer(NSmartContext context) {
        super(context);

        modelManager = NDataModelManager.getInstance(context.getKylinConfig(), context.getProject());
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        if (modelContexts == null || modelContexts.isEmpty())
            return;

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = compareWithFactTable(modelTree);
            if (model != null) {
                // found matched, then use it
                modelContext.setOrigModel(model);
                NDataModel targetModel = NDataModel.getCopyOf(model);
                initModel(targetModel);
                targetModel.getComputedColumnDescs().stream()
                        .forEach(cc -> context.getUsedCC().put(cc.getExpression(), cc));
                modelContext.setTargetModel(targetModel);
            }
        }

        // if manual maintain type and selected model is null, record error message
        final ProjectInstance projectInstance = NProjectManager.getInstance(getContext().getKylinConfig())
                .getProject(getContext().getProject());
        final Map<String, AccelerateInfo> sql2AccelerateInfo = context.getAccelerateInfoMap();

        if (projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            context.getModelContexts().forEach(modelCtx -> {
                if (modelCtx.withoutTargetModel()) {
                    modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                        AccelerateInfo accelerateInfo = sql2AccelerateInfo.get(olapContext.sql);
                        accelerateInfo.setBlockingCause(
                                new IllegalStateException("No model matches this query. In the model designer project, "
                                        + "the system is not allowed to suggest a new model accelerate this query."));
                    });
                }
            });
        }
    }

    private void initModel(NDataModel modelDesc) {
        KylinConfig kylinConfig = context.getKylinConfig();
        String project = context.getProject();
        modelDesc.init(kylinConfig, NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap(),
                Lists.<NDataModel> newArrayList(), false);
    }

    private NDataModel compareWithFactTable(ModelTree modelTree) {
        for (NDataModel model : modelManager.listModels()) {
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
                for (JoinTableDesc joinTableDesc : modelTree.getJoins().values()) {
                    modelTreeJoins.add(joinTableDesc.getJoin());
                    joinMap.put(joinTableDesc.getJoin().getPKSide(), joinTableDesc.getJoin().getFKSide());
                }
                for (Map.Entry<TableRef, TableRef> joinEntry : joinMap.entrySet()) {
                    if (!joinMap.containsKey(joinEntry.getValue())) {
                        factTblRef = joinEntry.getValue();
                        break;
                    }
                }
            }
            JoinsTree joinsTree = new JoinsTree(factTblRef, modelTreeJoins);
            //Map<String, String> matching = joinsTree.matches(model.getJoinsTree());
            //if (matching != null)
            if (GreedyModelTreesBuilder.matchJoinTree(model.getJoinsTree(), joinsTree))
                return true;
        }
        return false;
    }
}
