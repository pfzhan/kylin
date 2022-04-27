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
package io.kyligence.kap.metadata.cube;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import lombok.val;
import lombok.var;
import org.junit.rules.ExpectedException;

public class StreamingUtilsTest extends NLocalFileMetadataTestCase {
    public static final String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    private static String DATAFLOW_ID = MODEL_ID;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetToBuildLayouts() {
        val config = getTestConfig();
        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        var df = dfMgr.getDataflow(DATAFLOW_ID);
        var layoutSet = StreamingUtils.getToBuildLayouts(df);
        Assert.assertTrue(!layoutSet.isEmpty());

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments(SegmentStatusEnum.READY).toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);
        df = dfMgr.getDataflow(DATAFLOW_ID);
        layoutSet = StreamingUtils.getToBuildLayouts(df);
        Assert.assertTrue(!layoutSet.isEmpty());
    }

    @Test
    public void testGetJobId() {
        val jobType = "STREAMING_BUILD";
        val jobId = StreamingUtils.getJobId(MODEL_ID, jobType);
        Assert.assertEquals(MODEL_ID + "_build", jobId);
    }

    @Test
    public void testGetModelId() {
        val modelId = StreamingUtils.getModelId(MODEL_ID + "_build");
        Assert.assertEquals(MODEL_ID, modelId);
    }

    @Test
    public void testParseStreamingDuration() {
        var duration1 = StreamingUtils.parseStreamingDuration(null);
        Assert.assertEquals("30", duration1);

        var duration2 = StreamingUtils.parseStreamingDuration("");
        Assert.assertEquals("30", duration2);
        var duration3 = StreamingUtils.parseStreamingDuration("60");
        Assert.assertEquals("60", duration3);
    }

    @Test
    public void testParseSize() {
        Assert.assertEquals(32 * 1024 * 1024L, StreamingUtils.parseSize(null).longValue());

        Assert.assertEquals(32 * 1024 * 1024L, StreamingUtils.parseSize("").longValue());

        val ten_k = StreamingUtils.parseSize("10k").longValue();
        Assert.assertEquals(10240L, ten_k);
        val ten_kb = StreamingUtils.parseSize("10kb").longValue();
        Assert.assertEquals(10240L, ten_kb);

        val twenty_m = StreamingUtils.parseSize("20m").longValue();
        Assert.assertEquals(20 * 1024 * 1024L, twenty_m);
        val twenty_mb = StreamingUtils.parseSize("20mb").longValue();
        Assert.assertEquals(20 * 1024 * 1024L, twenty_mb);

        val ten_g = StreamingUtils.parseSize("10g").longValue();
        Assert.assertEquals(10 * 1024 * 1024 * 1024L, ten_g);
        val ten_gb = StreamingUtils.parseSize("10gb").longValue();
        Assert.assertEquals(10 * 1024 * 1024 * 1024L, ten_gb);
        thrown.expect(IllegalArgumentException.class);
        StreamingUtils.parseSize("3t");
    }

    @Test
    public void testParseTableRefreshInterval() {
        Assert.assertEquals(-1L, StreamingUtils.parseTableRefreshInterval(null).longValue());
        Assert.assertEquals(-1L, StreamingUtils.parseTableRefreshInterval(" ").longValue());

        Assert.assertEquals(30L, StreamingUtils.parseTableRefreshInterval("30m").longValue());
        Assert.assertEquals(2 * 60L, StreamingUtils.parseTableRefreshInterval("2h").longValue());
        Assert.assertEquals(3 * 24 * 60L, StreamingUtils.parseTableRefreshInterval("3d").longValue());

        thrown.expect(IllegalArgumentException.class);
        StreamingUtils.parseTableRefreshInterval("3t");
    }

    @Test
    public void testLocalMode() {
        val config = StreamingUtils.isLocalMode();
        Assert.assertEquals(false, config);
    }

    @Test
    public void testReplayAuditlog() {
        try {
            StreamingUtils.replayAuditlog();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testIsJobOnCluster() {
        val config = KylinConfig.getInstanceFromEnv();
        val result = StreamingUtils.isJobOnCluster(config);
        Assert.assertEquals(false, result);
    }

    @Test
    public void testSleep() {
        val start = System.currentTimeMillis();
        StreamingUtils.sleep(1000);
        Assert.assertTrue((System.currentTimeMillis() - start) >= 1000);
    }

    @Test
    public void testSleepException() {
        val start = System.currentTimeMillis();
        val t = new Thread(() -> {
            StreamingUtils.sleep(10000);
        });
        try {
            t.join(1000);
            t.interrupt();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InterruptedException);
        }
    }
}
