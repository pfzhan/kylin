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
package io.kyligence.kap.streaming.merge;

import java.io.IOException;

import io.kyligence.kap.streaming.util.StreamingTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import lombok.var;

public class NormalMergePolicyTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataflowManager mgr;
    private NormalMergePolicy normalMergePolicy;
    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    private static String DATAFLOW_ID = MODEL_ID;
    private static int thresholdOf1k = 1024;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        normalMergePolicy = new NormalMergePolicy();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    /**
     * test no matched seg list
     */
    @Test
    public void testSelectMatchedSegList() {
        val dataflow = mgr.getDataflow(DATAFLOW_ID);
        val segments = dataflow.getSegments().getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        Assert.assertTrue(segments.get(0).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(!segments.get(0).getAdditionalInfo().isEmpty());
        Assert.assertTrue(segments.get(1).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(!segments.get(1).getAdditionalInfo().isEmpty());
        Assert.assertTrue(segments.get(2).getStorageBytesSize() > thresholdOf1k);
        Assert.assertTrue(segments.get(2).getAdditionalInfo().isEmpty());

        var matchedSegList = normalMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k, 3);
        Assert.assertEquals(0, matchedSegList.size());

        var matchedSegList1 = normalMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k * 8, 3);
        Assert.assertEquals(0, matchedSegList1.size());
    }

    /**
     * L0 layer
     */
    @Test
    public void testSelectMatchedSegListOfLayer0() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30);
        df = setSegmentStorageSize(mgr, df, 2048L);
        val segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        var matchedSegList = normalMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 20, 3);
        Assert.assertEquals(3, matchedSegList.size());

        var matchedSegList1 = normalMergePolicy.selectMatchedSegList(segments, 0, thresholdOf1k * 3, 5);
        Assert.assertEquals(3, matchedSegList1.size());
    }

    /**
     * L1 layer
     */
    @Test
    public void testSelectMatchedSegListOfLayer1() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30, 1);
        df = setSegmentStorageSize(mgr, df, 2048L);
        val segments = df.getSegments();
        var matchedSegList = normalMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k * 20, 3);
        Assert.assertEquals(3, matchedSegList.size());

        var matchedSegList1 = normalMergePolicy.selectMatchedSegList(segments, 1, thresholdOf1k * 3, 5);
        Assert.assertEquals(3, matchedSegList1.size());
    }

}
