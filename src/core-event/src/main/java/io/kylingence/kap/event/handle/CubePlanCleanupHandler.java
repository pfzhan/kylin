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

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kylingence.kap.event.model.CubePlanCleanupEvent;
import io.kylingence.kap.event.model.EventContext;
import lombok.val;

public class CubePlanCleanupHandler extends AbstractEventHandler {
    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (CubePlanCleanupEvent) eventContext.getEvent();
        val kylinConfig = eventContext.getConfig();

        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, event.getProject());
        cubePlanManager.updateCubePlan(event.getCubePlanName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val oldRule = copyForWrite.getRuleBasedCuboidsDesc();
                val newRule = oldRule.getNewRuleBasedCuboid();
                if (newRule == null) {
                    return;
                }
                copyForWrite.setRuleBasedCuboidsDesc(newRule);
            }
        });
    }

    @Override
    public Class<?> getEventClassType() {
        return CubePlanCleanupEvent.class;
    }
}
