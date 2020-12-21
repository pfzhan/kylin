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
package io.kyligence.kap.tool.daemon.checker;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import io.kyligence.kap.tool.daemon.HealthChecker;
import io.kyligence.kap.tool.daemon.Worker;

public abstract class AbstractHealthChecker extends Worker implements HealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHealthChecker.class);

    private int priority = Integer.MAX_VALUE;

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int getPriority() {
        return this.priority;
    }

    protected void preCheck() {
        // nothing to do
    }

    protected void postCheck(CheckResult result) {
        // nothing to do
    }

    abstract CheckResult doCheck();

    @Override
    public CheckResult check() {
        logger.info("Checker:[{}] start to do check ...", this.getClass().getName());

        CheckResult result = null;
        try {
            preCheck();

            result = doCheck();

            postCheck(result);
            logger.info("Checker: [{}], do check finished! ", this.getClass().getName());
        } catch (Exception e) {
            logger.error("Checker: [{}], do check failed! ", this.getClass().getName(), e);
        }

        if (null == result) {
            String message = String.format(Locale.ROOT, "Checker: [%s] check result is null!",
                    this.getClass().getName());
            logger.warn(message);

            result = new CheckResult(CheckStateEnum.OTHER, message);
        }

        if (null == result.getCheckState()) {
            String message = String.format(Locale.ROOT, "Checker: [%s] check result state is null!",
                    this.getClass().getName());
            logger.warn(message);

            result.setCheckState(CheckStateEnum.OTHER);
            if (null == result.getReason()) {
                result.setReason(message);
            }
        }

        if (null == result.getReason()) {
            result.setReason("");
        }

        if (null == result.getCheckerName()) {
            result.setCheckerName(this.getClass().getName());
        }

        return result;
    }

}
