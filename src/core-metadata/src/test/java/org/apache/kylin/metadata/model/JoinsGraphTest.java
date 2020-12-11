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
import java.util.function.Supplier;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph orderIJFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factIJOrderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertTrue(orderIJFactGraph.match(factIJOrderGraph, new HashMap<String, String>()));

        JoinsGraph orderLJfactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factLJorderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertFalse(orderLJfactGraph.match(factLJorderGraph, new HashMap<String, String>()));
    }

    @Test
    public void testInnerJoinPartialMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph innerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        JoinsGraph innerAndInnerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertFalse(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<String, String>(),
                KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()));
        System.setProperty("kylin.query.match-partial-inner-join-model", "true");
        Assert.assertTrue(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<String, String>(),
                KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()));
    }

    @Test
    public void testMatchDupJoinTable() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .build();
        JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertTrue(graph1.match(graph2, new HashMap<String, String>()));
        Assert.assertTrue(graph2.match(graph1, new HashMap<String, String>()));

        JoinsGraph graph3 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" }).build();
        JoinsGraph graph4 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertTrue(graph3.match(graph4, new HashMap<String, String>()));
        Assert.assertTrue(graph4.match(graph3, new HashMap<String, String>()));
    }

    @Test
    public void testMatchLeft() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(modelJoinsGraph.match(modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(singleTblGraph.match(singleTblGraph, new HashMap<String, String>()));
        Assert.assertTrue(singleTblGraph.match(modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertFalse(modelJoinsGraph.match(singleTblGraph, new HashMap<String, String>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(noFactGraph.match(modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(factJoinGraph.match(modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertFalse(joinedFactGraph.match(factJoinGraph, new HashMap<String, String>()));
    }

    @Test
    public void testMatchInner() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(modelJoinsGraph.match(modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(singleTblGraph.match(singleTblGraph, new HashMap<String, String>()));
        Assert.assertFalse(singleTblGraph.match(modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertFalse(modelJoinsGraph.match(singleTblGraph, new HashMap<String, String>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(noFactGraph.match(modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(factJoinGraph.match(modelJoinsGraph, new HashMap<String, String>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertTrue(joinedFactGraph.match(factJoinGraph, new HashMap<String, String>()));
    }

    @Test
    public void testPartialMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(factJoinGraph.match(modelJoinsGraph, new HashMap<String, String>(), true));
    }

    @Test
    public void testNonEquiLeftJoinGraphMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<String, String>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<String, String>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, new HashMap<String, String>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, new HashMap<String, String>()));
        }
    }

    @Test
    public void testNonEquiInnerJoinGraphMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");

        Supplier<HashMap<String, String>> matchesMapSupplier = HashMap::new;

        {
            //exactly match
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match inner join is not allowed
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match inner join is not allowed-2
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, matchesMapSupplier.get()));
        }
    }
}