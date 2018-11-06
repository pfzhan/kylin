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

import java.util.ArrayList;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Functions;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.CubePlanRuleUpdateEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostCubePlanRuleUpdateEvent;
import io.kyligence.kap.event.model.RemoveCuboidByIdEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubePlanRuleUpdateHandler extends AbstractEventHandler implements DeriveEventMixin {

    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (CubePlanRuleUpdateEvent) eventContext.getEvent();

        val kylinConfig = eventContext.getConfig();

        handleDiffLayouts(event, kylinConfig);

    }

    private void handleDiffLayouts(CubePlanRuleUpdateEvent event, KylinConfig kylinConfig) throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, event.getProject());
        val ruleBasedCuboidDesc = cubePlanManager.getCubePlan(event.getCubePlanName()).getRuleBasedCuboidsDesc();
        val newRuleBasedCuboidDesc = ruleBasedCuboidDesc.getNewRuleBasedCuboid();
        if (newRuleBasedCuboidDesc == null) {
            log.debug("There is no new rule");
            return;
        }

        val originLayouts = ruleBasedCuboidDesc.genCuboidLayouts(false);
        val targetLayouts = ruleBasedCuboidDesc.getNewRuleBasedCuboid().genCuboidLayouts(false);

        if (!onlyRemoveMeasures(originLayouts, targetLayouts)) {
            val difference = Maps.difference(Maps.asMap(originLayouts, Functions.identity()),
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
                removeCuboidEvent.setIncludeManual(true);
                removeCuboidEvent.setLayoutIds(layoutIds);
                fireEvent(removeCuboidEvent, event, kylinConfig);
            }
        }

        // cleanup metadata
        val metadataEvent = new PostCubePlanRuleUpdateEvent();
        fireEvent(metadataEvent, event, kylinConfig);

    }

    private boolean onlyRemoveMeasures(Set<NCuboidLayout> originLayouts, Set<NCuboidLayout> targetLayouts) {
        if (originLayouts.size() != targetLayouts.size()) {
            return false;
        }
        for (NCuboidLayout originLayout : originLayouts) {
            boolean result = false;
            for (NCuboidLayout targetLayout : targetLayouts) {
                if (originLayout.containMeasures(targetLayout)) {
                    result = true;
                    break;
                }
            }
            if (!result) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Class<?> getEventClassType() {
        return CubePlanRuleUpdateEvent.class;
    }
}
