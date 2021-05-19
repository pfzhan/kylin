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

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PeakMergePolicyTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataflowManager mgr;
    private PeakMergePolicy peakMergePolicy;
    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73_rt";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        peakMergePolicy = new PeakMergePolicy();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    /**
     * test no matched seg list
     */
    public void testSelectMatchedSegList1() {
        val thresholdOf20k = StreamingUtils.parseSize("20k");
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 3);
        for (int i = 0; i < df.getSegments().size(); i++) {
            ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);

        var matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf20k, 3);
        Assert.assertEquals(0, matchedSegList.size());
    }

    @Test
    public void testSelectMatchedSegList() {
        val thresholdOf4k = StreamingUtils.parseSize("4k");
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 3);
        for (int i = 0; i < df.getSegments().size(); i++) {
            if (i < 2) {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
            } else {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 4096L);
            }
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        var matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 3);
        Assert.assertEquals(3, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 5);
        Assert.assertEquals(3, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, StreamingUtils.parseSize("20k"), 3);
        Assert.assertEquals(0, matchedSegList.size());

        matchedSegList = peakMergePolicy.selectMatchedSegList(segments, 0, thresholdOf4k, 2);
        Assert.assertEquals(0, matchedSegList.size());
    }

}
