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

package io.kyligence.kap.common.hystrix;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCircuitBreaker {

    private static final Logger logger = LoggerFactory.getLogger(NCircuitBreaker.class);

    private static final AtomicBoolean breakerStarted = new AtomicBoolean(false);

    private volatile static NBreakerConfig breakerConfig = null;

    public static void start(KapConfig verifiableProps) {

        synchronized (breakerStarted) {
            if (!breakerStarted.get()) {
                try {
                    breakerConfig = new NBreakerConfig(verifiableProps);

                    breakerStarted.set(true);

                    logger.info("kap circuit-breaker started");
                } catch (Exception e) {
                    logger.error("kap circuit-breaker start failed", e);
                }
            }
        }
    }

    public static void verifyProjectCreation(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfProject();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new CircuitBreakerException("Project num exceeded, threshold: " + threshold);
    }

    public static void verifyModelCreation(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfModel();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new CircuitBreakerException("Model num exceeded, threshold: " + threshold);
    }

    public static void verifyFavoriteQueryCreation(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfFavoriteQuery();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new CircuitBreakerException("FavoriteQuery num exceeded, threshold: " + threshold);
    }

    public static void verifySqlPatternToBlacklist(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfSqlPatternToBlacklist();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new CircuitBreakerException("SqlPatternToBlacklist num exceeded, threshold: " + threshold);
    }

    public static void verifyQueryResultRowCount(long current) {
        if (!isEnabled()) {
            return;
        }

        long threshold = breakerConfig.thresholdOfQueryResultRowCount();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new CircuitBreakerException("QueryResultRowCount num exceeded, threshold: " + threshold);
    }

    private static boolean isEnabled() {
        if (!breakerStarted.get()) {
            logger.warn("kap circuit-breaker not started");
            return false;
        }
        return breakerConfig.isBreakerEnabled();
    }
}
