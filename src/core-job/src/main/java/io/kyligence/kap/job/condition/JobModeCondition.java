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

package io.kyligence.kap.job.condition;

import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.KylinConfig;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;

@Slf4j
public class JobModeCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String beanName = "N/A";
        if (metadata instanceof AnnotationMetadata) {
            beanName = ((AnnotationMetadata) metadata).getClassName();
        } else if (metadata instanceof MethodMetadata) {
            beanName = ((MethodMetadata) metadata).getMethodName();
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isUTEnv()) {
            log.info("skip load bean = {} on UT env", beanName);
            return false;
        }

        if (kylinConfig.isDataLoadingNode()) {
            log.info("load bean = {} on yinglong-data-loading-booter", beanName);
            return true;
        }

        if (kylinConfig.isJobNode()) {
            log.info("load bean = {} on all/job mode", beanName);
            return true;
        }

        log.info("skip load bean = {} on query mode or not data-loading mirco-service", beanName);
        return false;

    }
}
