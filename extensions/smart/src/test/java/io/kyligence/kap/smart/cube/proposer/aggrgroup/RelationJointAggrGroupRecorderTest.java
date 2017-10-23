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

package io.kyligence.kap.smart.cube.proposer.aggrgroup;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.smart.common.SmartConfig;

public class RelationJointAggrGroupRecorderTest extends LocalFileMetadataTestCase {
    private static SmartConfig smartConfig;

    @Before
    public void setup() throws Exception {
        createTestMetadata();

        smartConfig = SmartConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void test1() {
        // A-B-C-D
        // A-E-F
        RelationJointAggrGroupRecorder recorder = new RelationJointAggrGroupRecorder(smartConfig);
        recorder.add("A", 0, "B", 0);
        recorder.add("A", 0, "C", 0);
        recorder.add("A", 0, "D", 0);
        recorder.add("B", 0, "C", 0);
        recorder.add("B", 0, "D", 0);
        recorder.add("C", 0, "D", 0);
        recorder.add("A", 0, "E", 0);
        recorder.add("A", 0, "F", 0);
        recorder.add("E", 0, "F", 0);
        List<List<String>> r = recorder.getResult();
        Assert.assertEquals(2, r.size());
        Assert.assertArrayEquals(new String[] { "A", "B", "C", "D" }, r.get(0).toArray());
        Assert.assertArrayEquals(new String[] { "E", "F" }, r.get(1).toArray());
    }

    @Test
    public void test2() {
        // A-B-C-D-F
        // B-E-F-G
        RelationJointAggrGroupRecorder recorder = new RelationJointAggrGroupRecorder(smartConfig);
        recorder.add("A", 0, "B", 0);
        recorder.add("A", 0, "C", 0);
        recorder.add("A", 0, "D", 0);
        recorder.add("A", 0, "F", 0);
        recorder.add("B", 0, "C", 0);
        recorder.add("B", 0, "D", 0);
        recorder.add("B", 0, "F", 0);
        recorder.add("C", 0, "D", 0);
        recorder.add("C", 0, "F", 0);
        recorder.add("D", 0, "F", 0);
        recorder.add("B", 0, "E", 0);
        recorder.add("B", 0, "F", 0);
        recorder.add("B", 0, "G", 0);
        recorder.add("E", 0, "F", 0);
        recorder.add("E", 0, "G", 0);
        recorder.add("F", 0, "G", 0);
        List<List<String>> r = recorder.getResult();
        Assert.assertEquals(2, r.size());
        Assert.assertArrayEquals(new String[] { "A", "B", "C", "D", "F" }, r.get(0).toArray());
        Assert.assertArrayEquals(new String[] { "E", "G" }, r.get(1).toArray());
    }
}
