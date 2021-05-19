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

import java.util.List;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import lombok.val;

public class MergePolicyTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataflowManager mgr;
    private MergePolicy mergePolicy;
    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        mergePolicy = new MergePolicy() {
            public int findStartIndex(List<NDataSegment> segList, Long thresholdOfSegSize) {
                return super.findStartIndex(segList, thresholdOfSegSize);
            }

            @Override
            public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer,
                    long thresholdOfSegSize, int numOfSeg) {
                return null;
            }

            @Override
            public boolean matchMergeCondition(long thresholdOfSegSize) {
                return false;
            }
        };
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testFindStartIndex() {
        KylinConfig testConfig = getTestConfig();
        val copy = createIndexPlan(testConfig, PROJECT, MODEL_ID, MODEL_ALIAS);
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.createDataflow(copy, "test_owner");

        df = createSegments(mgr, df, 30);
        for (int i = 0; i < df.getSegments().size(); i++) {
            if (i < 10) {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 4096L);
            } else {
                ReflectionUtils.setField(df.getSegments().get(i), "storageSize", 2048L);
            }
        }
        df = mgr.getDataflow(df.getId());
        val segments = df.getSegments();

        val startIndex = mergePolicy.findStartIndex(segments, 1024L);
        Assert.assertEquals(-1, startIndex);

        val startIndex1 = mergePolicy.findStartIndex(segments, 20480L);
        Assert.assertEquals(0, startIndex1);

        val startIndex2 = mergePolicy.findStartIndex(segments, 2048L);
        Assert.assertEquals(10, startIndex2);
    }

    @Test
    public void testIsThresholdOfSegSizeOver() {
        val thresholdOver = mergePolicy.isThresholdOfSegSizeOver(20480, 10240);
        Assert.assertEquals(true, thresholdOver);

        val thresholdOver1 = mergePolicy.isThresholdOfSegSizeOver(10240, 10240);
        Assert.assertEquals(false, thresholdOver1);
    }
}
