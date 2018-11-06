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
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.CubePlanRuleUpdateEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.ModelSemanticUpdateEvent;
import io.kyligence.kap.event.model.PostModelSemanticUpdateEvent;
import io.kyligence.kap.event.model.RemoveCuboidByIdEvent;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticUpdateHandler extends AbstractEventHandler implements DeriveEventMixin {

    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (ModelSemanticUpdateEvent) eventContext.getEvent();
        val cubeMgr = NCubePlanManager.getInstance(eventContext.getConfig(), event.getProject());
        val modelMgr = NDataModelManager.getInstance(eventContext.getConfig(), event.getProject());
        val dataflowManager = NDataflowManager.getInstance(eventContext.getConfig(), event.getProject());

        val matchingCubePlan = cubeMgr.findMatchingCubePlan(event.getModelName(), event.getProject(),
                eventContext.getConfig());
        val newModel = modelMgr.getDataModelDesc(event.getModelName());
        val originModel = event.getOriginModel();
        val allTables = NTableMetadataManager.getInstance(eventContext.getConfig(), event.getProject())
                .getAllTablesMap();
        originModel.init(eventContext.getConfig(), allTables, modelMgr.listModels(), false);

        if (isSignificantChange(originModel, newModel)) {
            handleMeasuresChanged(matchingCubePlan, newModel.getEffectiveMeasureMap().keySet(),
                    NCubePlan::setRuleBasedCuboidsDesc, cubeMgr);
            // do not need to fire this event, the follow logic will clear all segments
            removeUselessDimensions(matchingCubePlan, newModel.getEffectiveDimenionsMap().keySet(), false,
                    eventContext);
            handleReloadData(newModel, dataflowManager, eventContext);
            fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
            return;
        }
        val dimensionsOnlyAdded = newModel.getEffectiveDimenionsMap().keySet()
                .containsAll(originModel.getEffectiveDimenionsMap().keySet());
        val measuresNotChanged = CollectionUtils.isEqualCollection(newModel.getEffectiveMeasureMap().keySet(),
                originModel.getEffectiveMeasureMap().keySet());
        if (dimensionsOnlyAdded && measuresNotChanged) {
            fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
            return;
        }
        boolean needPostUpdate = true;
        // measure changed: does not matter to auto created cuboids' data, need refresh rule based cuboids
        if (!measuresNotChanged) {
            needPostUpdate = !handleMeasuresChanged(matchingCubePlan, newModel.getEffectiveMeasureMap().keySet(),
                    (cube, rule) -> {
                        try {
                            cube.setNewRuleBasedCuboid(rule);
                            fireEvent(new CubePlanRuleUpdateEvent(), event, eventContext.getConfig());
                        } catch (PersistentException e) {
                            log.info("persist failed", e);
                        }
                    }, cubeMgr);
        }
        // dimension deleted: previous step is remove dimensions in rule,
        //   so we only remove the auto created cuboids
        if (!dimensionsOnlyAdded) {
            removeUselessDimensions(matchingCubePlan, newModel.getEffectiveDimenionsMap().keySet(), true, eventContext);
        }
        if (needPostUpdate) {
            fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
        }
    }

    // if partitionDesc, mpCol, joinTable changed, we need reload data from datasource
    private boolean isSignificantChange(NDataModel originModel, NDataModel newModel) {
        return !Objects.equals(originModel.getPartitionDesc(), newModel.getPartitionDesc())
                || !Objects.equals(originModel.getMpColStrs(), newModel.getMpColStrs())
                || !Objects.equals(originModel.getJoinTables(), newModel.getJoinTables());
    }

    private boolean handleMeasuresChanged(NCubePlan cube, Set<Integer> measures,
            BiConsumer<NCubePlan, NRuleBasedCuboidsDesc> descConsumer, NCubePlanManager cubePlanManager)
            throws IOException {
        val savedCube = cubePlanManager.updateCubePlan(cube.getName(), copyForWrite -> {
            copyForWrite.setCuboids(copyForWrite.getCuboids().stream().filter(cuboid -> {
                val allMeasures = cuboid.getMeasures();
                allMeasures.removeAll(measures);
                return allMeasures.size() == 0;
            }).collect(Collectors.toList()));
            if (copyForWrite.getRuleBasedCuboidsDesc() == null) {
                return;
            }
            try {
                val newRule = JsonUtil.deepCopy(copyForWrite.getRuleBasedCuboidsDesc(), NRuleBasedCuboidsDesc.class);
                newRule.setMeasures(Lists.newArrayList(measures));
                descConsumer.accept(copyForWrite, newRule);
            } catch (IOException e) {
                log.warn("copy rule failed ", e);
            }
        });
        return savedCube.getRuleBasedCuboidsDesc() != null
                && savedCube.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid() != null;
    }

    private void removeUselessDimensions(NCubePlan cube, Set<Integer> availableDimensions, boolean triggerEvent,
            EventContext eventContext) throws PersistentException, IOException {
        val layoutIds = cube.getWhitelistCuboidLayouts().stream()
                .filter(layout -> layout.getColOrder().stream()
                        .anyMatch(col -> col < NDataModel.MEASURE_ID_BASE && !availableDimensions.contains(col)))
                .map(NCuboidLayout::getId).collect(Collectors.toList());
        if (layoutIds.isEmpty()) {
            return;
        }
        if (triggerEvent) {
            val removeEvent = new RemoveCuboidByIdEvent();
            removeEvent.setIncludeManual(true);
            removeEvent.setCubePlanName(cube.getName());
            removeEvent.setLayoutIds(layoutIds);
            fireEvent(removeEvent, eventContext.getEvent(), eventContext.getConfig());
        } else {
            val cubePlanManager = NCubePlanManager.getInstance(eventContext.getConfig(),
                    eventContext.getEvent().getProject());
            cubePlanManager.updateCubePlan(cube.getName(), copy -> {
                copy.setCuboids(copy.getCuboids().stream()
                        .filter(cuboid -> availableDimensions.containsAll(cuboid.getDimensions()))
                        .collect(Collectors.toList()));
            });
        }
    }

    private void handleReloadData(NDataModel model, NDataflowManager dataflowManager, EventContext eventContext)
            throws IOException, PersistentException {
        val df = dataflowManager.getDataflowByModelName(model.getName());
        dataflowManager.updateDataflow(df.getName(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.OFFLINE);
            copyForWrite.setSegments(new Segments<>());
        });
        val event = new AddCuboidEvent();
        event.setLayoutIds(
                df.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId).collect(Collectors.toList()));
        eventContext.getEvent().setCubePlanName(df.getCubePlanName());
        fireEvent(event, eventContext.getEvent(), eventContext.getConfig());
    }

    @Override
    public Class<?> getEventClassType() {
        return ModelSemanticUpdateEvent.class;
    }
}
