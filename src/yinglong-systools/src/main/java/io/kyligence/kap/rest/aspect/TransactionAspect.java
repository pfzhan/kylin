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

package io.kyligence.kap.rest.aspect;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class TransactionAspect {

    @Pointcut("@annotation(transaction)")
    public void callAt(Transaction transaction) {
        /// just implement it
    }

    @Around("callAt(transaction)")
    public Object around(ProceedingJoinPoint pjp, Transaction transaction) throws TransactionException {
        Object result = null;
        String unitName = UnitOfWork.GLOBAL_UNIT;
        if (transaction.project() != -1) {
            Object unitObject = pjp.getArgs()[transaction.project()];
            if (unitObject instanceof String) {
                unitName = unitObject.toString();
            } else if (unitObject instanceof TransactionProjectUnit) {
                unitName = ((TransactionProjectUnit) unitObject).transactionProjectUnit();
            }
        }

        if (!Objects.equals(UnitOfWork.GLOBAL_UNIT, unitName)) {
            ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(unitName);
            if (projectInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, unitName);
            }
        }

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(UnitOfWorkParams.builder().unitName(unitName)
                .readonly(transaction.readonly()).maxRetry(transaction.retry()).processor(() -> {
                    try {
                        return pjp.proceed();
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }).build());
    }
}
