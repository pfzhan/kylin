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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.MAINTENANCE_MODE_ENTER_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.MAINTENANCE_MODE_LEAVE_FAILED;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.response.MaintenanceModeResponse;

@Component("maintenanceModeService")
public class MaintenanceModeService extends BasicService implements MaintenanceModeSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Logger logger = LoggerFactory.getLogger(MaintenanceModeService.class);

    public void setMaintenanceMode(String reason) {
        aclEvaluate.checkIsGlobalAdmin();
        EpochManager epochMgr = EpochManager.getInstance();
        if (Boolean.FALSE.equals(epochMgr.setMaintenanceMode(reason))) {
            throw new KylinException(MAINTENANCE_MODE_ENTER_FAILED);
        }
        logger.info("System enter maintenance mode.");
    }

    public void unsetMaintenanceMode(String reason) {
        aclEvaluate.checkIsGlobalAdmin();
        EpochManager epochMgr = EpochManager.getInstance();
        if (Boolean.FALSE.equals(epochMgr.unsetMaintenanceMode(reason))) {
            throw new KylinException(MAINTENANCE_MODE_LEAVE_FAILED);
        }
        logger.info("System leave maintenance mode.");
    }

    public MaintenanceModeResponse getMaintenanceMode() {
        EpochManager epochMgr = EpochManager.getInstance();
        Pair<Boolean, String> maintenanceModeDetail = epochMgr.getMaintenanceModeDetail();
        return new MaintenanceModeResponse(maintenanceModeDetail.getFirst(), maintenanceModeDetail.getSecond());
    }

    @Override
    public boolean isMaintenanceMode() {
        return getMaintenanceMode().isMaintenanceMode();
    }
}