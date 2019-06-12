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

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MetricWriterStrategy {
    INSTANCE;
    public static final Logger logger = LoggerFactory.getLogger(MetricWriterStrategy.class);
    private static AtomicInteger failoverFlag = new AtomicInteger(0);
    public final static String CONFIG_KEY = "kap.metric.write-destination";

    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> metrics) {
        write(dbName, measurement, tags, metrics, System.currentTimeMillis());
    }

    public void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> metrics,
            long timestamp) {
        try {
            MetricWriter writer = MetricWriter.Factory.getInstance(getType());
            logger.trace("Use writer:" + writer.getType());
            writer.write(dbName, measurement, tags, metrics, timestamp);

            // reset failvoer flag
            failoverFlag.set(0);

        } catch (Throwable th) {
            logger.error("Error when write metrics, increment failover flag, current value is {}",
                    failoverFlag.incrementAndGet(), th);

            if (failoverFlag.get() > 10) {
                logger.error("Write failed more than 10 times, failover writer to 'BLACK_HOLE'");
                failover();
            }

        }
    }

    private void failover() {
        System.setProperty(CONFIG_KEY, MetricWriter.Type.BLACK_HOLE.name());
    }

    private String getType() {
        try {
            return KapConfig.getInstanceFromEnv().getMetricWriteDest();
        } catch (Exception e) {
            // In spark executor may can not get kylin config, use system property instead.
            return System.getProperty(CONFIG_KEY);
        }
    }
}
