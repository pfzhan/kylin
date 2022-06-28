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

package io.kyligence.kap.metadata.query;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class BigQueryThresholdUpdaterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void destroy() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBigQueryThresholdInit() {
        int instance = 1;
        int core = 1;
        BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(instance, core);
        Assert.assertTrue(BigQueryThresholdUpdater.getBigQueryThreshold() > 0);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "100000000");
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(instance, core);
            Assert.assertEquals(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold(),
                    BigQueryThresholdUpdater.getBigQueryThreshold());
        }
    }

    @Test
    public void testCollectQueryScanRowsAndTime() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "100000000");
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            int n = 1000;
            for (int i = 0; i < n; i++) {
                long duration = new Random().nextInt(10000) + KapConfig.getInstanceFromEnv().getBigQuerySecond() * 1000;
                if (i == n - 1) {
                    BigQueryThresholdUpdater.setLastUpdateTime(System.currentTimeMillis()
                            - KapConfig.getInstanceFromEnv().getBigQueryThresholdUpdateIntervalSecond() * 1000);
                }
                BigQueryThresholdUpdater.collectQueryScanRowsAndTime(new Random().nextInt(10000),
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() - 1);
                BigQueryThresholdUpdater.collectQueryScanRowsAndTime(duration,
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
            }
            long collectScanRows = KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1;
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                    () -> Assert.assertEquals(collectScanRows, BigQueryThresholdUpdater.getBigQueryThreshold()));
        }
    }
}
