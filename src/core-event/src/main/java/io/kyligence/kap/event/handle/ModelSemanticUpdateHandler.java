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
package io.kyligence.kap.event.handle;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kyligence.kap.event.model.CubePlanRuleUpdateEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostModelSemanticUpdateEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.PersistentException;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.metadata.model.ModelStatus;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.event.model.ModelSemanticUpdateEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticUpdateHandler extends AbstractEventHandler implements DeriveEventMixin {

    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (ModelSemanticUpdateEvent) eventContext.getEvent();
        val cubeMgr = NCubePlanManager.getInstance(eventContext.getConfig(), event.getProject());
        val modelMgr = NDataModelManager.getInstance(eventContext.getConfig(), event.getProject());

        val cubes = cubeMgr.findMatchingCubePlan(event.getModelName(), event.getProject(), eventContext.getConfig());
        val newModel = modelMgr.getDataModelDesc(event.getModelName());
        val originModel = event.getOriginModel();

        modelMgr.updateDataModel(event.getModelName(), model -> model.setModelStatus(ModelStatus.NOT_READY));
        if (!Objects.equals(originModel.getPartitionDesc(), newModel.getPartitionDesc())
                || !Objects.equals(originModel.getMpColStrs(), newModel.getMpColStrs())
                || !Objects.equals(originModel.getJoinTables(), newModel.getJoinTables())) {
            fireCubeEvents(cubes, eventContext, cube -> new RefreshSegmentEvent());
        }
        val dimensionsOnlyAdded = newModel.getEffectiveDimenionsMap().keySet()
                .containsAll(originModel.getEffectiveDimenionsMap().keySet());
        val measuresNotChanged = CollectionUtils.isEqualCollection(newModel.getEffectiveMeasureMap().keySet(),
                originModel.getEffectiveMeasureMap().keySet());
        if (dimensionsOnlyAdded && measuresNotChanged) {
            return;
        }
        // measure changed: does not matter to auto created cuboids' data, need refresh rule based cuboids
        if (!measuresNotChanged) {
            handleMeasuresChanged(cubes, newModel.getEffectiveMeasureMap().keySet(), cubeMgr);
            fireCubeEvents(cubes, eventContext, cube -> {
                if (cube.getRuleBasedCuboidsDesc() != null)
                    return new CubePlanRuleUpdateEvent();
                return null;
            });
        }
        // dimension deleted: previous step is remove dimensions in rule,
        //   so we only remove the auto created cuboids
        if (!dimensionsOnlyAdded) {
            removeUselessDimensions(cubes, newModel.getEffectiveDimenionsMap().keySet(), cubeMgr);
        }
        val postUpdateEvent = new PostModelSemanticUpdateEvent();
        fireEvent(postUpdateEvent, event, eventContext.getConfig());
    }

    private void handleMeasuresChanged(List<NCubePlan> cubes, Set<Integer> measures, NCubePlanManager cubePlanManager)
            throws IOException {
        for (NCubePlan cube : cubes) {
            cubePlanManager.updateCubePlan(cube.getName(), copyForWrite -> {
                for (NCuboidLayout layout : copyForWrite.getWhitelistCuboidLayouts()) {
                    layout.getCuboidDesc().setMeasures(layout.getCuboidDesc().getMeasures().stream()
                            .filter(measures::contains).collect(Collectors.toList()));
                    layout.setColOrder(layout.getColOrder().stream()
                            .filter(col -> col < NDataModel.MEASURE_ID_BASE || measures.contains(col))
                            .collect(Collectors.toList()));
                }
                if (copyForWrite.getRuleBasedCuboidsDesc() == null) {
                    return;
                }
                try {
                    val newRule = JsonUtil.deepCopy(copyForWrite.getRuleBasedCuboidsDesc(),
                            NRuleBasedCuboidsDesc.class);
                    newRule.setMeasures(Lists.newArrayList(measures));
                    copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
                } catch (IOException e) {
                    log.warn("copy rule failed ", e);
                }
            });
        }

    }

    private void removeUselessDimensions(List<NCubePlan> cubes, Set<Integer> dimensions,
            NCubePlanManager cubePlanManager) throws IOException {
        for (NCubePlan cube : cubes) {
            cubePlanManager.updateCubePlan(cube.getName(), copyForWrite -> {
                for (NCuboidLayout layout : copyForWrite.getWhitelistCuboidLayouts()) {
                    layout.getCuboidDesc().setDimensions(layout.getCuboidDesc().getDimensions().stream()
                            .filter(dimensions::contains).collect(Collectors.toList()));
                    layout.setColOrder(layout.getColOrder().stream()
                            .filter(col -> col >= NDataModel.MEASURE_ID_BASE || dimensions.contains(col))
                            .collect(Collectors.toList()));
                }
            });
        }
    }

    private void fireCubeEvents(List<NCubePlan> cubes, EventContext context, Function<NCubePlan, Event> eventCreator)
            throws PersistentException {
        for (NCubePlan cube : cubes) {
            val refreshEvent = eventCreator.apply(cube);
            if (refreshEvent == null) {
                continue;
            }
            refreshEvent.setCubePlanName(cube.getName());
            fireEvent(refreshEvent, context.getEvent(), context.getConfig());
        }
    }

    @Override
    public Class<?> getEventClassType() {
        return ModelSemanticUpdateEvent.class;
    }
}
