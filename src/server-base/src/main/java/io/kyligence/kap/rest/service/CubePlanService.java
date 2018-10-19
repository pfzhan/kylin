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
package io.kyligence.kap.rest.service;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.CubePlanUpdateEvent;
import lombok.val;

@Service("cubePlanService")
public class CubePlanService extends BasicService {

    public NCubePlan updateRuleBasedCuboid(final UpdateRuleBasedCuboidRequest request) throws IOException, PersistentException {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val cubePlanManager = NCubePlanManager.getInstance(kylinConfig, request.getProject());
        val eventManager = EventManager.getInstance(kylinConfig, request.getProject());

        val cubePlan = cubePlanManager.updateCubePlan(request.getCubePlanName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val newRuleBasedCuboid = new NRuleBasedCuboidsDesc();
                BeanUtils.copyProperties(request, newRuleBasedCuboid);
                newRuleBasedCuboid.setCubePlan(copyForWrite);
                newRuleBasedCuboid.init();
                if (copyForWrite.getRuleBasedCuboidsDesc() == null) {
                    copyForWrite.setRuleBasedCuboidsDesc(new NRuleBasedCuboidsDesc());
                }
                copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRuleBasedCuboid);
            }
        });
        val event = new CubePlanUpdateEvent();
        event.setApproved(true);
        event.setProject(request.getProject());
        event.setCubePlanName(cubePlan.getName());
        event.setModelName(cubePlan.getModelName());
        eventManager.post(event);
        return cubePlan;
    }


}
