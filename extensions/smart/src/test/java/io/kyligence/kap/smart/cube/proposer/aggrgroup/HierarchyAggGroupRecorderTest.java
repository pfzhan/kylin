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

import org.junit.Assert;
import org.junit.Test;

public class HierarchyAggGroupRecorderTest {
    @Test
    public void test1() {
        // A->B->C->D
        // A->E->F
        HierarchyAggGroupRecorder recorder = new HierarchyAggGroupRecorder();
        recorder.add("A", "B");
        recorder.add("A", "C");
        recorder.add("A", "D");
        recorder.add("B", "C");
        recorder.add("B", "D");
        recorder.add("C", "D");
        recorder.add("A", "E");
        recorder.add("A", "F");
        recorder.add("E", "F");

        List<List<String>> result = recorder.getResult();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(4, result.get(0).size());
        Assert.assertEquals(2, result.get(1).size());
    }

    @Test
    public void test2() {
        // A->B->C->D
        // E->B->F
        HierarchyAggGroupRecorder recorder = new HierarchyAggGroupRecorder();
        recorder.add("A", "B");
        recorder.add("A", "C");
        recorder.add("A", "D");
        recorder.add("A", "F");
        recorder.add("B", "C");
        recorder.add("B", "D");
        recorder.add("B", "F");
        recorder.add("C", "D");
        recorder.add("E", "B");
        recorder.add("E", "C");
        recorder.add("E", "D");
        recorder.add("E", "F");

        List<List<String>> result = recorder.getResult();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(4, result.get(0).size());
        Assert.assertEquals(2, result.get(1).size());
    }
}
