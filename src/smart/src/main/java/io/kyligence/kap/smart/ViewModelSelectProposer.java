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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ViewModelSelectProposer extends AbstractProposer {

    private final NDataModelManager dataModelManager;

    protected ViewModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public void execute() {
        Map<String, Collection<OLAPContext>> modelViewOLAPContextMap = proposeContext.getModelViewOLAPContextMap();
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        if (MapUtils.isEmpty(modelViewOLAPContextMap) || null == modelContexts) {
            return;
        }
        Map<String, AbstractContext.ModelContext> existedModelContextsMap = modelContexts.stream()
                .filter(e -> null != e.getOriginModel())
                .collect(Collectors.toMap(e -> e.getOriginModel().getAlias(), v -> v, (v1, v2) -> v1));
        Map<String, NDataModel> aliasModelMap = proposeContext.getOriginModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, Function.identity()));
        modelViewOLAPContextMap.forEach((modelAlias, olapContexts) -> {
            if (existedModelContextsMap.containsKey(modelAlias)) {
                addToExistedModelContext(existedModelContextsMap.get(modelAlias), olapContexts);
            } else {
                NDataModel dataModel = aliasModelMap.get(modelAlias);
                if (null != dataModel) {
                    createNewModelContext(dataModel, olapContexts);
                }
            }
        });
    }

    private void addToExistedModelContext(AbstractContext.ModelContext existedContext,
            Collection<OLAPContext> olapContexts) {
        existedContext.getModelTree().getOlapContexts().addAll(olapContexts);
    }

    private void createNewModelContext(NDataModel dataModel, Collection<OLAPContext> olapContexts) {
        AbstractContext.ModelContext modelContext = proposeContext.createModelContext(
                new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext)
                        .build(olapContexts, dataModel.getRootFactTable().getTableDesc()));
        setModelContextModel(modelContext, dataModel);
        proposeContext.getModelContexts().add(modelContext);
    }

    @Override
    public String getIdentifierName() {
        return "ViewModelSelectProposer";
    }

    private void setModelContextModel(AbstractContext.ModelContext modelContext, NDataModel dataModel) {
        modelContext.setOriginModel(dataModel);
        NDataModel targetModel = dataModelManager.copyBySerialization(dataModel);
        targetModel.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
        modelContext.setTargetModel(targetModel);
        targetModel.getComputedColumnDescs().forEach(cc -> modelContext.getUsedCC().put(cc.getExpression(), cc));
    }
}
