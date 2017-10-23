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

public class FragmentJointAggrGroupRecorderTest extends LocalFileMetadataTestCase {
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
    public void testByName() {
        FragmentJointAggrGroupRecorder recorder = new FragmentJointAggrGroupRecorder(smartConfig);
        recorder.add("IS_A", 100);
        recorder.add("IS_B", 100);
        recorder.add("IS_C", 100);
        recorder.add("IS_D", 100);
        recorder.add("A_TYPE", 100);
        recorder.add("B_TYPE", 100);
        recorder.add("C_TYPE", 100);
        recorder.add("IS_TYPE", 100);

        List<List<String>> result = recorder.getResult(0);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(5, result.get(0).size());
        Assert.assertEquals(3, result.get(1).size());
    }

    @Test
    public void testRandom() {
        FragmentJointAggrGroupRecorder recorder = new FragmentJointAggrGroupRecorder(smartConfig);
        recorder.add("A", 2);
        recorder.add("B", 2);
        recorder.add("C", 3);
        recorder.add("D", 4);
        recorder.add("E", 5);
        recorder.add("F", 6);
        recorder.add("G", 7);
        recorder.add("H", 8);

        List<List<String>> result = recorder.getResult(0);
        Assert.assertFalse(result.isEmpty());
    }
}
