/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.controller.topology;

import java.util.Collections;
import java.util.List;

import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.sequencesql.DiskResultCache;
import io.kyligence.kap.rest.sequencesql.ResultOpt;
import io.kyligence.kap.rest.sequencesql.SequenceNodeOutput;
import io.kyligence.kap.rest.sequencesql.SequenceOpt;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopology;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopologyManager;

public class SequenceTopologyTest {
    @Test
    public void basicTest() {
        SequenceTopologyManager manager = new SequenceTopologyManager(new DiskResultCache(), 86400000);
        manager.addTopology(1L, 1);
        SequenceTopology topology = manager.getTopology(1L, 1);

        {
            int stepID = topology.addStep("query0", SequenceOpt.INIT, null);
            Assert.assertEquals(0, stepID);
            SQLResponse response0 = new SQLResponse();
            List<List<String>> results0 = Lists.newArrayList();
            results0.add(Collections.singletonList("1"));
            results0.add(Collections.singletonList("2"));
            results0.add(Collections.singletonList("3"));
            response0.setResults(results0);
            int count0 = topology.updateSQLNodeResult(stepID, response0);
            Assert.assertEquals(3, count0);
            SequenceNodeOutput sequeneFinalResult0 = topology.getSequeneFinalResult();
            Assert.assertEquals(3, sequeneFinalResult0.size());
            System.out.println(topology);
        }

        {
            int stepID = topology.addStep("query1", SequenceOpt.APPEND, ResultOpt.INTERSECT);
            Assert.assertEquals(1, stepID);
            SQLResponse response1 = new SQLResponse();
            List<List<String>> results1 = Lists.newArrayList();
            results1.add(Collections.singletonList("2"));
            results1.add(Collections.singletonList("8"));
            response1.setResults(results1);
            int count1 = topology.updateSQLNodeResult(stepID, response1);
            Assert.assertEquals(1, count1);
            SequenceNodeOutput sequeneFinalResult1 = topology.getSequeneFinalResult();
            Assert.assertEquals(1, sequeneFinalResult1.size());
            System.out.println(topology);
        }

        {
            int stepID = topology.addStep("query2", SequenceOpt.APPEND, ResultOpt.BACKWARD_EXCEPT);
            Assert.assertEquals(2, stepID);
            SQLResponse response2 = new SQLResponse();
            List<List<String>> results2 = Lists.newArrayList();
            results2.add(Collections.singletonList("3"));
            results2.add(Collections.singletonList("8"));
            results2.add(Collections.singletonList("2"));
            response2.setResults(results2);
            int count2 = topology.updateSQLNodeResult(stepID, response2);
            Assert.assertEquals(0, count2);
            SequenceNodeOutput sequeneFinalResult2 = topology.getSequeneFinalResult();
            Assert.assertEquals(0, sequeneFinalResult2.size());
            System.out.println(topology);
        }

        {
            SQLResponse response3 = new SQLResponse();
            List<List<String>> results3 = Lists.newArrayList();
            results3.add(Collections.singletonList("3"));
            results3.add(Collections.singletonList("8"));
            response3.setResults(results3);
            int stepID = topology.updateStep(2, "newquery2", SequenceOpt.UPDATE, null);
            int count3 = topology.updateSQLNodeResult(stepID, response3);
            Assert.assertEquals(1, count3);
            SequenceNodeOutput sequeneFinalResult3 = topology.getSequeneFinalResult();
            Assert.assertEquals(1, sequeneFinalResult3.size());
            System.out.println(topology);
        }

        {
            SQLResponse response4 = new SQLResponse();
            List<List<String>> results4 = Lists.newArrayList();
            results4.add(Collections.singletonList("1"));
            results4.add(Collections.singletonList("2"));
            results4.add(Collections.singletonList("8"));
            response4.setResults(results4);
            int stepID = topology.updateStep(0, "newquery0", SequenceOpt.UPDATE, null);
            int count4 = topology.updateSQLNodeResult(stepID, response4);
            Assert.assertEquals(1, count4);
            SequenceNodeOutput sequeneFinalResult4 = topology.getSequeneFinalResult();
            Assert.assertEquals(1, sequeneFinalResult4.size());
            System.out.println(topology);
        }

        {
            SQLResponse response5 = new SQLResponse();
            List<List<String>> results5 = Lists.newArrayList();
            results5.add(Collections.singletonList("1"));
            results5.add(Collections.singletonList("2"));
            results5.add(Collections.singletonList("8"));
            response5.setResults(results5);
            int stepID = topology.updateStep(1, "newquery1", SequenceOpt.UPDATE, null);
            int count5 = topology.updateSQLNodeResult(stepID, response5);
            Assert.assertEquals(2, count5);
            SequenceNodeOutput sequeneFinalResult5 = topology.getSequeneFinalResult();
            Assert.assertEquals(2, sequeneFinalResult5.size());
            System.out.println(topology);
        }

        {
            SQLResponse response5 = new SQLResponse();
            List<List<String>> results5 = Lists.newArrayList();
            results5.add(Collections.singletonList("1"));
            results5.add(Collections.singletonList("2"));
            results5.add(Collections.singletonList("8"));
            results5.add(Collections.singletonList("10"));
            response5.setResults(results5);
            int stepID = topology.updateStep(1, "newnewquery1", SequenceOpt.UPDATE, ResultOpt.UNION);
            int count5 = topology.updateSQLNodeResult(stepID, response5);
            Assert.assertEquals(3, count5);
            SequenceNodeOutput sequeneFinalResult5 = topology.getSequeneFinalResult();
            Assert.assertEquals(3, sequeneFinalResult5.size());
            System.out.println(topology);
        }

        {
            SQLResponse response5 = new SQLResponse();
            List<List<String>> results5 = Lists.newArrayList();
            results5.add(Collections.singletonList("1"));
            results5.add(Collections.singletonList("2"));
            results5.add(Collections.singletonList("8"));
            results5.add(Collections.singletonList("11"));
            response5.setResults(results5);
            int stepID = topology.updateStep(0, "newnewquery0", SequenceOpt.UPDATE, null);
            int count5 = topology.updateSQLNodeResult(stepID, response5);
            Assert.assertEquals(4, count5);
            SequenceNodeOutput sequeneFinalResult5 = topology.getSequeneFinalResult();
            Assert.assertEquals(4, sequeneFinalResult5.size());
            System.out.println(topology);
        }

    }

    @Test(expected = IllegalStateException.class)
    public void testError() {

        SequenceTopologyManager manager = new SequenceTopologyManager(new DiskResultCache(), 86400000);
        manager.addTopology(1L, 1);
        SequenceTopology topology = manager.getTopology(1L, 1);

        topology.addStep("query0", SequenceOpt.APPEND, ResultOpt.INTERSECT);
    }

    @Test
    public void testExpire() throws InterruptedException {

        DiskResultCache diskResultCache = new DiskResultCache();
        SequenceTopologyManager manager = new SequenceTopologyManager(diskResultCache, 1000);
        manager.addTopology(1L, 1);
        SequenceTopology topology = manager.getTopology(1L, 1);

        int sqlID = topology.addStep("query0", SequenceOpt.INIT, null);
        Assert.assertEquals(0, sqlID);
        SQLResponse response0 = new SQLResponse();
        List<List<String>> results0 = Lists.newArrayList();
        results0.add(Collections.singletonList("1"));
        results0.add(Collections.singletonList("2"));
        results0.add(Collections.singletonList("3"));
        response0.setResults(results0);
        int count0 = topology.updateSQLNodeResult(sqlID, response0);
        Assert.assertEquals(3, count0);
        SequenceNodeOutput sequeneFinalResult0 = topology.getSequeneFinalResult();
        Assert.assertEquals(3, sequeneFinalResult0.size());
        System.out.println(topology);

        Assert.assertEquals(1, diskResultCache.getSize());

        Thread.sleep(5000);
        manager.cleanup();

        Assert.assertEquals(0, diskResultCache.getSize());

    }

}
