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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;

@Component("epochService")
public class EpochService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(EpochService.class);

    @Autowired(required = false)
    public AclEvaluate aclEvaluate;

    public void updateEpoch(List<String> projects, boolean force, boolean client) {
        if (!client)
            aclEvaluate.checkIsGlobalAdmin();

        EpochManager epochMgr = EpochManager.getInstance();

        NProjectManager projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (projects.isEmpty()) {
            projects.add(EpochManager.GLOBAL);
            projects.addAll(projectMgr.listAllProjects().stream().map(ProjectInstance::getName).collect(Collectors.toList()));
        }
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String project : projects) {
            if (!rgManager.instanceHasPermissionToOwnEpochTarget(project, AddressUtil.getLocalInstance())) {
                continue;
            }
            logger.info("update epoch {}", project);
            epochMgr.updateEpochWithNotifier(project, force);
        }
    }

    public void updateAllEpochs(boolean force, boolean client) {
        if (!client)
            aclEvaluate.checkIsGlobalAdmin();
        NProjectManager projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> prjs = projectMgr.listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toList());
        prjs.add(UnitOfWork.GLOBAL_UNIT);
        updateEpoch(prjs, force, client);
    }

    public boolean isMaintenanceMode() {
        EpochManager epochMgr = EpochManager.getInstance();
        return epochMgr.isMaintenanceMode();
    }
}