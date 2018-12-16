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

package org.apache.kylin.metadata.model;

import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class JoinsGraphTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }
    
    @Test
    public void testMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("nmodel_basic");
        
        JoinsGraph orderIJFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factIJOrderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertTrue(JoinsGraph.match(orderIJFactGraph, factIJOrderGraph, new HashMap<String, String>()));
        
        JoinsGraph orderLJfactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factLJorderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertFalse(JoinsGraph.match(orderLJfactGraph, factLJorderGraph, new HashMap<String, String>()));
    }
    
    @Test
    public void testMatchLeft() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("nmodel_basic");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(JoinsGraph.match(modelJoinsGraph, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(JoinsGraph.match(singleTblGraph, singleTblGraph, new HashMap<String, String>()));
        Assert.assertTrue(JoinsGraph.match(singleTblGraph, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertFalse(JoinsGraph.match(modelJoinsGraph, singleTblGraph, new HashMap<String, String>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(JoinsGraph.match(noFactGraph, modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(JoinsGraph.match(factJoinGraph, modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertFalse(JoinsGraph.match(joinedFactGraph, factJoinGraph, new HashMap<String, String>()));
    }

    @Test
    public void testMatchInner() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(JoinsGraph.match(modelJoinsGraph, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(JoinsGraph.match(singleTblGraph, singleTblGraph, new HashMap<String, String>()));
        Assert.assertFalse(JoinsGraph.match(singleTblGraph, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertFalse(JoinsGraph.match(modelJoinsGraph, singleTblGraph, new HashMap<String, String>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(JoinsGraph.match(noFactGraph, modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(JoinsGraph.match(factJoinGraph, modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertTrue(JoinsGraph.match(joinedFactGraph, factJoinGraph, new HashMap<String, String>()));
    }
    
    @Test
    public void testPartialMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(JoinsGraph.match(factJoinGraph, modelJoinsGraph, new HashMap<String, String>(), true));
    }
    
    private class MockJoinGraphBuilder {
        private NDataModel modelDesc;
        private TableRef root;
        private List<JoinDesc> joins;
        
        public MockJoinGraphBuilder(NDataModel modelDesc, String rootName) {
            this.modelDesc = modelDesc;
            this.root = modelDesc.findTable(rootName);
            Assert.assertNotNull(root);
            this.joins = Lists.newArrayList();
        }

        private JoinDesc mockJoinDesc(String joinType, String[] fkCols, String[] pkCols) {
            JoinDesc joinDesc = new JoinDesc();
            joinDesc.setType(joinType);
            joinDesc.setPrimaryKey(fkCols);
            joinDesc.setPrimaryKey(pkCols);
            TblColRef[] fkColRefs = new TblColRef[fkCols.length];
            for (int i = 0; i < fkCols.length; i++) {
                fkColRefs[i] = modelDesc.findColumn(fkCols[i]);
            }
            TblColRef[] pkColRefs = new TblColRef[pkCols.length];
            for (int i = 0; i < pkCols.length; i++) {
                pkColRefs[i] = modelDesc.findColumn(pkCols[i]);
            }
            joinDesc.setForeignKeyColumns(fkColRefs);
            joinDesc.setPrimaryKeyColumns(pkColRefs);
            return joinDesc;
        }
        
        public MockJoinGraphBuilder innerJoin(String[] fkCols, String[] pkCols) {
            joins.add(mockJoinDesc("INNER", fkCols, pkCols));
            return this;
        }
        
        public MockJoinGraphBuilder leftJoin(String[] fkCols, String[] pkCols) {
            joins.add(mockJoinDesc("LEFT", fkCols, pkCols));
            return this;
        }
        
        public JoinsGraph build() {
            return new JoinsGraph(root, joins);
        }
    }

}