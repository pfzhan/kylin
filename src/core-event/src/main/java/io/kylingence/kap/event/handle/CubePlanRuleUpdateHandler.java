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
package io.kylingence.kap.event.handle;

import java.util.ArrayList;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.CubePlanRuleUpdateEvent;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.PostCubePlanRuleUpdateEvent;
import io.kylingence.kap.event.model.RemoveCuboidByIdEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubePlanRuleUpdateHandler extends AbstractEventHandler {

    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (CubePlanRuleUpdateEvent) eventContext.getEvent();

        val kylinConfig = eventContext.getConfig();

        if (event.isModelChanged()) {
            handleModelChanged(event, kylinConfig);
        } else {
            handleDiffLayouts(event, kylinConfig);
        }

    }

    private void handleModelChanged(CubePlanRuleUpdateEvent event, KylinConfig kylinConfig) {
        // TODO
    }

    private void handleDiffLayouts(CubePlanRuleUpdateEvent event, KylinConfig kylinConfig) throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, event.getProject());
        val ruleBasedCuboidDesc = cubePlanManager.getCubePlan(event.getCubePlanName()).getRuleBasedCuboidsDesc();
        val newRuleBasedCuboidDesc = ruleBasedCuboidDesc.getNewRuleBasedCuboid();
        if (newRuleBasedCuboidDesc == null) {
            log.debug("There is no new rule");
            return;
        }

        val allLayouts = ruleBasedCuboidDesc.genCuboidLayouts();
        val originLayouts = FluentIterable.from(allLayouts).filter(new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout input) {
                return input.getVersion() == 1;
            }
        }).toSet();
        val targetLayouts = FluentIterable.from(allLayouts).filter(new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout input) {
                return input.getVersion() == 2;
            }
        }).toSet();

        val difference = Maps.difference(
                Maps.asMap(originLayouts, Functions.identity()),
                Maps.asMap(targetLayouts, Functions.identity()));

        // new cuboid
        if (difference.entriesOnlyOnRight().size() > 0) {
            val addCuboidEvent = new AddCuboidEvent();
            val layoutIds = new ArrayList<Long>();
            for (NCuboidLayout addedLayout : difference.entriesOnlyOnRight().keySet()) {
                layoutIds.add(addedLayout.getId());
            }
            addCuboidEvent.setLayoutIds(layoutIds);
            fireEvent(addCuboidEvent, event, kylinConfig);
        }

        // old cuboid
        if (difference.entriesOnlyOnLeft().size() > 0) {
            val removeCuboidEvent = new RemoveCuboidByIdEvent();
            val layoutIds = new ArrayList<Long>();
            for (NCuboidLayout removedLayout : difference.entriesOnlyOnLeft().keySet()) {
                layoutIds.add(removedLayout.getId());
            }
            removeCuboidEvent.setLayoutIds(layoutIds);
            fireEvent(removeCuboidEvent, event, kylinConfig);
        }

        // cleanup metadata
        val metadataEvent = new PostCubePlanRuleUpdateEvent();
        fireEvent(metadataEvent, event, kylinConfig);

    }

    private void fireEvent(Event newEvent, CubePlanRuleUpdateEvent originEvent, KylinConfig kylinConfig) throws PersistentException {
        val eventManager = EventManager.getInstance(kylinConfig, originEvent.getProject());
        val eventAutoApproved = kylinConfig.getEventAutoApproved();

        newEvent.setApproved(eventAutoApproved);
        newEvent.setProject(originEvent.getProject());
        newEvent.setModelName(originEvent.getModelName());
        newEvent.setCubePlanName(originEvent.getCubePlanName());
        newEvent.setSegmentRange(originEvent.getSegmentRange());
        newEvent.setParentId(originEvent.getId());
        eventManager.post(newEvent);
    }

    @Override
    public Class<?> getEventClassType() {
        return CubePlanRuleUpdateEvent.class;
    }
}
