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
package io.kyligence.kap.streaming;

import io.kyligence.kap.streaming.util.ReflectionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingMergeEntryTest extends StreamingTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StreamingMergeEntryTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    /**
     * test l0 merge
     */
    @Test
    public void testMergeSegmentLayer0() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
        streamingMergeEntry.setNumberOfSeg(10);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 11);
        df = setSegmentStorageSize(mgr, df, 1024);

        shutdownStreamingMergeJob();
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        df = mgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
        Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
    }

    /**
     * test normal merge: L0 merge & L1 merge
     */
    @Test
    public void testMergeSegment() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 10);
        setSegmentStorageSize(mgr, df, 1024);

        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            latch.countDown();
        }
        df = mgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals("2", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
        Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
    }

    /**
     * test no merge for L1 layer
     */
    @Test
    public void testMergeSegmentLayer1() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 10, 1);
        df = setSegmentStorageSize(mgr, df, 1024);

        shutdownStreamingMergeJob();
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        df = mgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(10, df.getSegments().size());
        df.getSegments().stream().forEach(item -> Assert.assertEquals("1", item.getAdditionalInfo().get("file_layer")));
    }

    @Test
    public void testMergeSegmentOfCatchup1() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 16);
        setSegmentStorageSize(mgr, df, 1024);

        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(2, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testMergeSegmentOfCatchup2() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();

        streamingMergeEntry.setThresholdOfSegSize(14 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 16);
        Assert.assertEquals(16, df.getSegments().size());
        setSegmentStorageSize(mgr, df, 1024);

        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(3, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
            Assert.assertTrue(df.getSegments().get(2).getAdditionalInfo().isEmpty());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testMergeSegmentOfCatchup3() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();

        streamingMergeEntry.setThresholdOfSegSize(30 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 21, null, copyForWrite -> {
            for (int i = 0; i < 2; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "2");
            }
            for (int i = 2; i < 5; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "1");
            }
        });
        setSegmentStorageSize(mgr, df, 1024);
        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(2, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testMergeSegmentOfCatchup4() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();

        streamingMergeEntry.setThresholdOfSegSize(16 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(SparkSession.builder().master("local").appName("mergejob").getOrCreate());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 19, null, copyForWrite -> {
            for (int i = 0; i < 1; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "2");
            }
            for (int i = 1; i < 2; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "1");
            }
        });
        setSegmentStorageSize(mgr, df, 1024);
        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(4, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
            Assert.assertTrue(df.getSegments().get(2).getAdditionalInfo().isEmpty());
            Assert.assertTrue(df.getSegments().get(3).getAdditionalInfo().isEmpty());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testMergeSegmentOfPeak1() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();

        streamingMergeEntry.setThresholdOfSegSize(5 * 1024);
        streamingMergeEntry.setNumberOfSeg(5);
        streamingMergeEntry.setSparkSession(createSparkSession());
        Assert.assertNotNull(streamingMergeEntry.getSparkSession());
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(DATAFLOW_ID);
        df = createSegments(mgr, df, 6, null, copyForWrite -> {
            for (int i = 0; i < 2; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "2");
            }
            for (int i = 2; i < 4; i++) {
                val seg = copyForWrite.getSegments().get(i);
                seg.getAdditionalInfo().put("file_layer", "1");
            }
            for (int i = 4; i < 6; i++) {
                val seg = copyForWrite.getSegments().get(i);
            }
        });
        for (int i = 0; i < 4; i++) {
            val seg = df.getSegments().get(i);
            setSegmentStorageSize(seg, 2048L);
        }
        for (int i = 4; i < 6; i++) {
            val seg = df.getSegments().get(i);
            setSegmentStorageSize(seg, 5 * 1024L);
        }
        mgr.getDataflow(df.getId());
        val latch = new CountDownLatch(1);
        shutdownStreamingMergeJob(latch);
        try {
            streamingMergeEntry.schedule(PROJECT, DATAFLOW_ID);
            streamingMergeEntry.getSparkSession().stop();
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(2, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testNoClearHdfsFiles() {
        val config = getTestConfig();
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));

        val start = new AtomicLong(System.currentTimeMillis() - 60000);
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        mgr.updateDataflow(update);
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds1.size());
    }

    @Test
    public void testClearHdfsFiles() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-clean-interval", "0h");
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));
        val start = new AtomicLong(System.currentTimeMillis() - 60000);
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        mgr.updateDataflow(update);
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(0, removeSegIds1.size());
    }

    @Test
    public void testIsJobOnCluster() {
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        Assert.assertFalse(streamingMergeEntry.isJobOnCluster());
    }

    @Ignore
    @Test
    public void testShutdown() {
        val result = StreamingMergeEntry.shutdown();
        Assert.assertFalse(result);
    }
}
