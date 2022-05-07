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

import static io.kyligence.kap.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.StreamingTestConstant;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.parser.AbstractDataParser;
import io.kyligence.kap.streaming.CreateStreamingFlatTable;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.common.CreateFlatTableEntry;
import io.kyligence.kap.streaming.common.MergeJobEntry;
import io.kyligence.kap.streaming.common.MicroBatchEntry;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;
import scala.Tuple2;
import scala.collection.mutable.ArrayBuffer;

public class StreamingDFMergeJobTest extends StreamingTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StreamingDFMergeJobTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    private AbstractDataParser getParser() throws Exception {
        return AbstractDataParser.getDataParser(DEFAULT_PARSER_NAME, Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testStreamingMergeSegment() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        KylinBuildEnv.getOrCreate(config);

        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());

        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        var df = dfMgr.getDataflow(DATAFLOW_ID);
        // cleanup all segments first
        var update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        df = dfMgr.getDataflow(df.getId());
        Assert.assertEquals(0, df.getSegments().size());

        val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan());
        val layouts = StreamingUtils.getToBuildLayouts(df);
        Preconditions.checkState(CollectionUtils.isNotEmpty(layouts), "layouts is empty", layouts);
        val nSpanningTree = NSpanningTreeFactory.fromLayouts(layouts, DATAFLOW_ID);

        val ss = SparkSession.builder().master("local").appName("test").getOrCreate();
        CreateFlatTableEntry flatTableEntry = new CreateFlatTableEntry(flatTableDesc, null, nSpanningTree, ss, null,
                null, null, getParser());
        val flatTable = new CreateStreamingFlatTable(flatTableEntry);

        val dataset = flatTable.generateStreamingDataset(config);
        val builder = new StreamingDFBuildJob(PROJECT);

        val streamingEntry = new StreamingEntry();
        streamingEntry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "1000", "", "xx", DEFAULT_PARSER_NAME });
        streamingEntry.setSparkSession(ss);
        val sr1 = createSegmentRange(0L, 10L, 3, 100L, 200);
        val microBatchEntry = new MicroBatchEntry(dataset, 0, "SSB_TOPIC_0_DOT_0_LO_PARTITIONCOLUMN", flatTable, df,
                nSpanningTree, builder, sr1);
        val minMaxBuffer = new ArrayBuffer<Tuple2<Object, Object>>(1);
        streamingEntry.processMicroBatch(microBatchEntry, minMaxBuffer);
        df = dfMgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(1, df.getSegments().size());

        val sr2 = createSegmentRange(10L, 20L, 3, 200L, 300);
        microBatchEntry.setSegmentRange(sr2);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        streamingEntry.processMicroBatch(microBatchEntry, minMaxBuffer);
        df = dfMgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(2, df.getSegments().size());

        val sr3 = createSegmentRange(20L, 30L, 3, 300L, 400);
        microBatchEntry.setSegmentRange(sr3);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        streamingEntry.processMicroBatch(microBatchEntry, minMaxBuffer);
        df = dfMgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(3, df.getSegments().size());

        val sr4 = createSegmentRange(30L, 40L, 3, 400L, 500);
        microBatchEntry.setSegmentRange(sr4);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        streamingEntry.processMicroBatch(microBatchEntry, minMaxBuffer);
        df = dfMgr.getDataflow(DATAFLOW_ID);
        Assert.assertEquals(4, df.getSegments().size());

        val mergeJobEntry = createMergeJobEntry(dfMgr, df, ss, PROJECT);
        df = dfMgr.getDataflow(df.getId());
        val merger = new StreamingDFMergeJob();
        KylinBuildEnv.getOrCreate(config);
        try {
            merger.streamingMergeSegment(mergeJobEntry);
            Assert.assertEquals(100L, mergeJobEntry.afterMergeSegmentSourceCount());
            Assert.assertEquals(5, df.getSegments().size());
            Assert.assertEquals("1",
                    df.getSegment(mergeJobEntry.afterMergeSegment().getId()).getAdditionalInfo().get("file_layer"));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);

        }

    }

    protected MergeJobEntry createMergeJobEntry(NDataflowManager mgr, NDataflow df, SparkSession ss, String project) {

        val retainSegments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 40L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 500L));
        val copy = mgr.getDataflow(df.getId()).copy();
        val afterMergeSeg = mgr.mergeSegments(copy, rangeToMerge, true, 1, null);
        val updatedSegments = retainSegments.stream().map(seg -> {
            return df.getSegment(seg.getId());
        }).collect(Collectors.toList());
        val globalMergeTime = new AtomicLong(System.currentTimeMillis());
        val mergeJobEntry = new MergeJobEntry(ss, project, df.getId(), 100L, globalMergeTime, updatedSegments,
                afterMergeSeg);
        return mergeJobEntry;
    }

    public NDataflow prepareSegment() {
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(df.getId());

        val seg1 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 10L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 200L)));
        val seg2 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 20L,
                createKafkaPartitionsOffset(3, 200L), createKafkaPartitionsOffset(3, 300L)));
        val seg3 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(20L, 30L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 400L)));
        val seg4 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(30L, 40L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L)));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        seg3.setStatus(SegmentStatusEnum.READY);
        seg4.setStatus(SegmentStatusEnum.READY);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToUpdateSegs(seg1, seg2, seg3, seg4);
        List<NDataLayout> layouts = Lists.newArrayList();
        val dfCopy = df;
        indexManager.getIndexPlan(DATAFLOW_ID).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg1.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg2.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg3.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg4.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        mgr.updateDataflow(update2);
        return mgr.getDataflow(df.getId());
    }
}
