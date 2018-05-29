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

package io.kyligence.kap.common.metric;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MetricWriterStrategy {
    INSTANCE;
    public static final Logger logger = LoggerFactory.getLogger(MetricWriterStrategy.class);
    private static AtomicInteger retryTime = new AtomicInteger();
    private final static String CONFIG_KEY = "kap.metric.diagnosis.graph-writer-type";

    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> metrics) {
        try {
            String type;
            // In spark executor may can not get kylin config, use system property instead.
            if (!StringUtils.isEmpty(KylinConfig.getKylinHomeWithoutWarn())) {
                type = KapConfig.getInstanceFromEnv().diagnosisMetricWriterType();
            } else {
                type = System.getProperty(CONFIG_KEY);
            }
            MetricWriter writer = MetricWriter.Factory.getInstance(type);
            logger.trace("Use writer:" + writer.getType());
            writer.write(dbName, measurement, tags, metrics);
            retryTime.set(0);
        } catch (Throwable th) {
            if (retryTime.getAndIncrement() >= 10) {
                logger.error("Write failed more than 10 times, change writer to black_hole");
                if (!StringUtils.isEmpty(KylinConfig.getKylinHomeWithoutWarn())) {
                    KylinConfig.getInstanceFromEnv().setProperty(CONFIG_KEY, "BLACK_HOLE");
                } else {
                    System.setProperty(CONFIG_KEY, "BLACK_HOLE");
                }
                return;
            }
            logger.error("Error when getting JVM info, error times:" + retryTime.get(), th);
        }
    }
}
