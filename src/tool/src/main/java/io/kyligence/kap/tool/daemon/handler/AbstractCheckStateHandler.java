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
package io.kyligence.kap.tool.daemon.handler;

import com.google.common.base.Preconditions;
import io.kyligence.kap.common.util.SecretKeyUtil;
import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateHandler;
import io.kyligence.kap.tool.daemon.HandleResult;
import io.kyligence.kap.tool.daemon.HandleStateEnum;
import io.kyligence.kap.tool.daemon.ServiceOpLevelEnum;
import io.kyligence.kap.tool.daemon.Worker;
import io.kyligence.kap.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCheckStateHandler extends Worker implements CheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCheckStateHandler.class);

    public boolean upGradeQueryService() {
        return opQueryService(ServiceOpLevelEnum.QUERY_UP_GRADE);
    }

    public boolean downGradeQueryService() {
        return opQueryService(ServiceOpLevelEnum.QUERY_DOWN_GRADE);
    }

    private boolean opQueryService(ServiceOpLevelEnum opLevelEnum) {
        try {
            if (null == getKgSecretKey()) {
                setKgSecretKey(SecretKeyUtil.readKGSecretKeyFromFile());
            }
            Preconditions.checkNotNull(getKgSecretKey(), "kg secret key is null!");

            if (null == getKE_PID()) {
                setKEPid(ToolUtil.getKylinPid());
            }
            byte[] encryptedToken = SecretKeyUtil.generateEncryptedTokenWithPid(getKgSecretKey(), getKE_PID());
            getRestClient().downOrUpGradeKE(opLevelEnum.getOpType(), encryptedToken);
        } catch (Exception e) {
            logger.error("Failed to operate service {}", opLevelEnum.getOpType(), e);
            return false;
        }
        return true;
    }

    abstract HandleResult doHandle(CheckResult checkResult);

    @Override
    public HandleResult handle(CheckResult checkResult) {
        logger.info("Handler: [{}], Health Checker: [{}] check result is {}, message: {}", this.getClass().getName(),
                checkResult.getCheckerName(), checkResult.getCheckState(), checkResult.getReason());

        HandleResult result;
        try {
            result = doHandle(checkResult);
            logger.info("Handler: [{}] handle the check result success ...", this.getClass().getName());
        } catch (Exception e) {
            logger.error("Failed to do handle!", e);
            result = new HandleResult(HandleStateEnum.HANDLE_FAILED);
        }

        if (null == result) {
            result = new HandleResult(HandleStateEnum.HANDLE_WARN);
        }

        if (null == result.getHandleState()) {
            result.setHandleState(HandleStateEnum.HANDLE_WARN);
        }

        return result;
    }

}
