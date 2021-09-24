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
package io.kyligence.kap.streaming.jobs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;

public class StreamingJobUtilsTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStreamingKylinConfig() {
        val config = getTestConfig();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val jobId = DATAFLOW_ID + "_build";

        val jobMeta = mgr.getStreamingJobByUuid(jobId);
        val params = jobMeta.getParams();
        params.put("kylin.streaming.spark-conf.spark.executor.memoryOverhead", "1g");
        params.put("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "300");
        params.put("kylin.streaming.table-refresh-interval", "1h");

        val kylinConfig = StreamingJobUtils.getStreamingKylinConfig(config, params, jobMeta.getModelId(), PROJECT);
        Assert.assertFalse(kylinConfig.getStreamingSparkConfigOverride().isEmpty());
        Assert.assertFalse(kylinConfig.getStreamingKafkaConfigOverride().isEmpty());
        Assert.assertEquals("1h", kylinConfig.getStreamingTableRefreshInterval());
    }

    @Test
    public void testGetStreamingKylinConfigOfProject() {
        val config = getTestConfig();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val jobId = DATAFLOW_ID + "_build";

        val jobMeta = mgr.getStreamingJobByUuid(jobId);
        val params = jobMeta.getParams();
        config.setProperty("kylin.streaming.spark-conf.spark.executor.memoryOverhead", "1g");
        config.setProperty("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "300");
        config.setProperty("kylin.streaming.table-refresh-interval", "30m");

        val kylinConfig = StreamingJobUtils.getStreamingKylinConfig(config, params, "", PROJECT);
        Assert.assertFalse(kylinConfig.getStreamingSparkConfigOverride().isEmpty());
        Assert.assertFalse(kylinConfig.getStreamingKafkaConfigOverride().isEmpty());
        Assert.assertEquals("30m", kylinConfig.getStreamingTableRefreshInterval());
    }
}
