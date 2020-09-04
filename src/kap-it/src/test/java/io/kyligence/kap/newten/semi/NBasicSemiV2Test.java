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

package io.kyligence.kap.newten.semi;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.query.util.QueryHisStoreUtil;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NBasicSemiV2Test extends SemiAutoTestBase {

    private static final long QUERY_TIME = 1595520000000L;

    private JdbcRawRecStore jdbcRawRecStore;
    private RDBMSQueryHistoryDAO queryHistoryDAO;
    RawRecService rawRecommendation;

    @Before
    public void setup() throws Exception {
        super.setup();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        rawRecommendation = new RawRecService();
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void teardown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");

        log.debug("clean SqlSessionFactory...");
        Class<RawRecStoreUtil> clazz = RawRecStoreUtil.class;
        Field sqlSessionFactory = clazz.getDeclaredField("sqlSessionFactory");
        sqlSessionFactory.setAccessible(true);
        sqlSessionFactory.set(null, null);
        System.out.println(sqlSessionFactory.get(null));
        sqlSessionFactory.setAccessible(false);
        log.debug("clean SqlSessionFactory success");

        Class<QueryHisStoreUtil> qhClazz = QueryHisStoreUtil.class;
        Field qhSqlSessionFactory = qhClazz.getDeclaredField("sqlSessionFactory");
        qhSqlSessionFactory.setAccessible(true);
        qhSqlSessionFactory.set(null, null);
        System.out.println(qhSqlSessionFactory.get(null));
        qhSqlSessionFactory.setAccessible(false);
        log.debug("clean SqlSessionFactory success");
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testBasic() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // test
        String[] sqls = { "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact group by lstg_format_name, price *  5" };

        val context = NSmartMaster.genOptRecommendationSemiV2(getTestConfig(), getProject(), sqls, null);
        List<AbstractContext.NModelContext> modelContexts = context.getModelContexts();
        AbstractContext.NModelContext modelContext = modelContexts.get(0);
        val ccRecItemMap = modelContext.getCcRecItemMap();
        val dimensionRecItemMap = modelContext.getDimensionRecItemMap();
        val measureRecItemMap = modelContext.getMeasureRecItemMap();
        val indexRexItemMap = modelContext.getIndexRexItemMap();

        Assert.assertEquals(2, ccRecItemMap.size());
        Assert.assertEquals(2, dimensionRecItemMap.size());
        Assert.assertEquals(2, measureRecItemMap.size());
        Assert.assertEquals(1, indexRexItemMap.size());

        String key = "SUM__TEST_KYLIN_FACT$8";
        Assert.assertTrue(measureRecItemMap.containsKey(key));
        measureRecItemMap.remove(key);
        final String measureName = measureRecItemMap.keySet().iterator().next();
        final String oneCCUuid = measureName.split("__")[1];
        Assert.assertTrue(ccRecItemMap.containsKey(oneCCUuid));
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE",
                ccRecItemMap.get(oneCCUuid).getCc().getExpression());
    }

    @Test
    public void testNotGenerateSuggestionsWhenExistIndex() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        String query1 = "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by lstg_format_name, price *  5";
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), new String[] { query1 });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // collect recommendation
        String[] sqls1 = { "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by lstg_format_name, price *  5" };
        val context = NSmartMaster.genOptRecommendationSemiV2(getTestConfig(), getProject(), sqls1, null);
        AbstractContext.NModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(0, modelContext.getCcRecItemMap().size());
        Assert.assertEquals(0, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(0, modelContext.getMeasureRecItemMap().size());
        Assert.assertEquals(0, modelContext.getIndexRexItemMap().size());
    }

    @Test
    public void testCollectRecItemWhenDifferenceBatch() {



    }

    @Test
    public void testCollectRecItemWithMultipleModels() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare 3 initial model
        String query1 = "select price from test_kylin_fact left join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id";
        String query2 = "select price from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "left join test_country on test_account.account_country = test_country.country";
        String query3 = "select price from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { query1, query2, query3 });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // collect recommendation
        String[] sqls = {
                "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                        + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                        + "group by lstg_format_name, price *  5",
                "select count(item_count*price), sum(price * 5) "
                        + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id ",
                "select max(item_count*price),min(price * 5) from test_kylin_fact inner join test_account "
                        + "on test_kylin_fact.seller_id = test_account.account_id "
                        + "left join test_country on test_account.account_country = test_country.country" };
        val context = NSmartMaster.genOptRecommendationSemiV2(getTestConfig(), getProject(), sqls, null);
        List<AbstractContext.NModelContext> modelContexts = context.getModelContexts();

        // validate
        collectCCRecItemWithMultipleModels(modelContexts);
        collectDimentionRecItemWithMultipleModels(modelContexts);
        collectMeasureRecItemWithMultipleModels(modelContexts);
        collectIndexRecItemWithMultipleModels(modelContexts);
    }

    public void collectCCRecItemWithMultipleModels(List<AbstractContext.NModelContext> modelContexts) {
        Map<String, CCRecItemV2> ccRecItemMap1 = modelContexts.get(0).getCcRecItemMap();
        Assert.assertEquals(2, ccRecItemMap1.size());
        ArrayList<CCRecItemV2> ccRecItemList1 = new ArrayList<>(ccRecItemMap1.values());
        ccRecItemList1.sort(new Comparator<CCRecItemV2>() {
            @Override
            public int compare(CCRecItemV2 o1, CCRecItemV2 o2) {
                return o1.getCc().hashCode() - o2.getCc().hashCode();
            }
        });
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * 5", ccRecItemList1.get(0).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList1.get(0).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", ccRecItemList1.get(0).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(21,4)", ccRecItemList1.get(0).getCc().getDatatype());
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE",
                ccRecItemList1.get(1).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList1.get(1).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`",
                ccRecItemList1.get(1).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(30,4)", ccRecItemList1.get(1).getCc().getDatatype());

        Map<String, CCRecItemV2> ccRecItemMap2 = modelContexts.get(1).getCcRecItemMap();
        Assert.assertEquals(2, ccRecItemMap2.size());
        ArrayList<CCRecItemV2> ccRecItemList2 = new ArrayList<>(ccRecItemMap2.values());
        ccRecItemList2.sort(new Comparator<CCRecItemV2>() {
            @Override
            public int compare(CCRecItemV2 o1, CCRecItemV2 o2) {
                return o1.getCc().hashCode() - o2.getCc().hashCode();
            }
        });
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * 5", ccRecItemList2.get(0).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList2.get(0).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", ccRecItemList2.get(0).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(21,4)", ccRecItemList2.get(0).getCc().getDatatype());
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE",
                ccRecItemList2.get(1).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList2.get(1).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`",
                ccRecItemList2.get(1).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(30,4)", ccRecItemList2.get(1).getCc().getDatatype());
    }

    public void collectDimentionRecItemWithMultipleModels(List<AbstractContext.NModelContext> modelContexts) {
        Map<String, DimensionRecItemV2> dimensionRecItemMap1 = modelContexts.get(0).getDimensionRecItemMap();
        Assert.assertEquals(2, dimensionRecItemMap1.size());
        ArrayList<DimensionRecItemV2> dimensionRecItemList1 = new ArrayList<>(dimensionRecItemMap1.values());
        dimensionRecItemList1.sort(new Comparator<DimensionRecItemV2>() {
            @Override
            public int compare(DimensionRecItemV2 o1, DimensionRecItemV2 o2) {
                return o1.getColumn().getId() - o2.getColumn().getId();
            }
        });
        Assert.assertEquals("varchar(4096)", dimensionRecItemList1.get(0).getDataType());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                dimensionRecItemList1.get(0).getColumn().getAliasDotColumn());
        Assert.assertEquals(4, dimensionRecItemList1.get(0).getColumn().getId());
        Assert.assertEquals("decimal(21,4)", dimensionRecItemList1.get(1).getDataType());
        Assert.assertEquals(18, dimensionRecItemList1.get(1).getColumn().getId());

        Map<String, DimensionRecItemV2> dimensionRecItemMap2 = modelContexts.get(1).getDimensionRecItemMap();
        Assert.assertEquals(0, dimensionRecItemMap2.size());
    }

    public void collectMeasureRecItemWithMultipleModels(List<AbstractContext.NModelContext> modelContexts) {
        Map<String, MeasureRecItemV2> measureRecItemMap1 = modelContexts.get(0).getMeasureRecItemMap();
        Assert.assertEquals(4, measureRecItemMap1.size());
        ArrayList<MeasureRecItemV2> measureRecItemList = new ArrayList<>(measureRecItemMap1.values());
        measureRecItemList.sort(new Comparator<MeasureRecItemV2>() {
            @Override
            public int compare(MeasureRecItemV2 o1, MeasureRecItemV2 o2) {
                return o1.getMeasure().getId() - o2.getMeasure().getId();
            }
        });
        Assert.assertEquals(100001, measureRecItemList.get(0).getMeasure().getId());
        Assert.assertEquals("decimal(38,4)", measureRecItemList.get(0).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(0).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList.get(0)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100002, measureRecItemList.get(1).getMeasure().getId());
        Assert.assertEquals("decimal(29,4)", measureRecItemList.get(1).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(1).getMeasure().getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE",
                measureRecItemList.get(1).getMeasure().getFunction().getParameters().get(0).getColRef().getIdentity());

        Assert.assertEquals(100003, measureRecItemList.get(2).getMeasure().getId());
        Assert.assertEquals("bigint", measureRecItemList.get(2).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("COUNT", measureRecItemList.get(2).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList.get(2)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100004, measureRecItemList.get(3).getMeasure().getId());
        Assert.assertEquals("decimal(31,4)", measureRecItemList.get(3).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(3).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", measureRecItemList.get(3).getMeasure().getFunction()
                .getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Map<String, MeasureRecItemV2> measureRecItemMap2 = modelContexts.get(1).getMeasureRecItemMap();
        Assert.assertEquals(2, measureRecItemMap2.size());
        ArrayList<MeasureRecItemV2> measureRecItemList2 = new ArrayList<>(measureRecItemMap2.values());
        measureRecItemList2.sort(new Comparator<MeasureRecItemV2>() {
            @Override
            public int compare(MeasureRecItemV2 o1, MeasureRecItemV2 o2) {
                return o1.getMeasure().getId() - o2.getMeasure().getId();
            }
        });
        Assert.assertEquals(100001, measureRecItemList2.get(0).getMeasure().getId());
        Assert.assertEquals("decimal(30,4)", measureRecItemList2.get(0).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("MAX", measureRecItemList2.get(0).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList2.get(0)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100002, measureRecItemList2.get(1).getMeasure().getId());
        Assert.assertEquals("decimal(21,4)", measureRecItemList2.get(1).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("MIN", measureRecItemList2.get(1).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", measureRecItemList2.get(1).getMeasure().getFunction()
                .getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());
    }

    public void collectIndexRecItemWithMultipleModels(List<AbstractContext.NModelContext> modelContexts) {
        Map<String, LayoutRecItemV2> indexRexItemMap1 = modelContexts.get(0).getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap1.size());
        ArrayList<LayoutRecItemV2> layoutRecItemList1 = new ArrayList<>(indexRexItemMap1.values());
        layoutRecItemList1.sort(new Comparator<LayoutRecItemV2>() {
            @Override
            public int compare(LayoutRecItemV2 o1, LayoutRecItemV2 o2) {
                return (int) (o1.getLayout().getId() - o2.getLayout().getId());
            }
        });
        Assert.assertEquals("[18, 4, 100000, 100001, 100002]",
                layoutRecItemList1.get(0).getLayout().getColOrder().toString());
        Assert.assertEquals("[100000, 100003, 100004]", layoutRecItemList1.get(1).getLayout().getColOrder().toString());

        Map<String, LayoutRecItemV2> indexRexItemMap2 = modelContexts.get(1).getIndexRexItemMap();
        Assert.assertEquals(1, indexRexItemMap2.size());
        ArrayList<LayoutRecItemV2> layoutRecItemList2 = new ArrayList<>(indexRexItemMap2.values());
        Assert.assertEquals("[100000, 100001, 100002]", layoutRecItemList2.get(0).getLayout().getColOrder().toString());
    }

    @Test
    public void testGenerateRawRecommendationsLayoutMetric() {
        // prepare two origin model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact", "select name from test_country" });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);
        List<NDataModel> originModels = smartContext.getOriginModels();

        // generate raw recommendations for origin model
        rawRecommendation.generateRawRecommendations(getProject(), queryHistories());

        // check raw recommendations layoutMetric
        List<RawRecItem> recOfCountry = jdbcRawRecStore.listAll(getProject(), originModels.get(0).getUuid(), 0, 10);
        Assert.assertEquals(2, recOfCountry.size());
        RawRecItem dimRecItemOfCountry = jdbcRawRecStore.queryById(1);
        RawRecItem layoutRecItemOfCountry = jdbcRawRecStore.queryById(3);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, dimRecItemOfCountry.getType());
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRecItemOfCountry.getType());
        Assert.assertEquals(1, layoutRecItemOfCountry.getLayoutMetric().getFrequencyMap().getDateFrequency()
                .get(QUERY_TIME).intValue());

        List<RawRecItem> recOfFact = jdbcRawRecStore.listAll(getProject(), originModels.get(1).getUuid(), 0, 10);
        Assert.assertEquals(2, recOfFact.size());
        RawRecItem dimRecItemOfFact = jdbcRawRecStore.queryById(2);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, dimRecItemOfFact.getType());
        RawRecItem layoutRecItemOfFact = jdbcRawRecStore.queryById(4);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRecItemOfFact.getType());
        Assert.assertEquals(1,
                layoutRecItemOfFact.getLayoutMetric().getFrequencyMap().getDateFrequency().get(QUERY_TIME).intValue());
    }
    
    @Test
    public void testTransferToCCRawRecItem() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare two origin model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(price+1) from test_kylin_fact " });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);
        List<NDataModel> originModels = smartContext.getOriginModels();

        // generate raw recommendations for origin model
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select sum(price+1+1) from test_kylin_fact group by price * 3");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);
        rawRecommendation.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
    }

    @Test
    public void testMarkFailAccelerateMessageToQueryHistory() {
        // prepare query history
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, getProject()));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, getProject()));

        // create a fail accelerate and succeed accelerate, then mark to query history
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select ACCOUNT_SELLER_LEVEL from TEST_ACCOUNT" });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select ACCOUNT_BUYER_LEVEL from TEST_ACCOUNT");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setSql("select country from test_country");
        queryHistory2.setId(2);

        rawRecommendation.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1, queryHistory2));

        // check if mark succeed
        List<QueryHistory> allQueryHistories = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, allQueryHistories.size());
        Assert.assertEquals(1, allQueryHistories.get(0).getId());
        Assert.assertNull(allQueryHistories.get(0).getQueryHistoryInfo().getErrorMsg());
        // fail accelerate mark succeed
        Assert.assertEquals(2, allQueryHistories.get(1).getId());
        Assert.assertEquals("There is no compatible model to accelerate this sql.",
                allQueryHistories.get(1).getQueryHistoryInfo().getErrorMsg());
    }

    @Test
    public void testMergeAggIndexOfSameDimensionForSemiV2() {
        String project = "cc_test";

        // create origin model
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { "SELECT LO_CUSTKEY FROM SSB.LINEORDER limit 10" });
        val originSmartMaster = new NSmartMaster(context1);
        originSmartMaster.runUtWithContext(smartUtHook);

        // suggest model
        String[] sqls = new String[] { "SELECT min(LO_CUSTKEY) FROM LINEORDER limit 10",
                "SELECT max(LO_CUSTKEY) FROM SSB.LINEORDER limit 10" };
        AbstractContext proposeContext = new ModelReuseContextOfSemiV2(getTestConfig(), project, sqls, true);
        val smartMaster = new NSmartMaster(proposeContext);
        smartMaster.runSuggestModel();

        // two layout will merge to one layout rec
        Assert.assertEquals(1, proposeContext.getModelContexts().get(0).getIndexRexItemMap().size());
        for (LayoutRecItemV2 layoutRecItem : proposeContext.getModelContexts().get(0).getIndexRexItemMap().values()) {
            Assert.assertEquals(2, layoutRecItem.getLayout().getMeasureIds().size());
        }
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select CAL_DT from test_kylin_fact");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setSql("select country from test_country");
        queryHistory2.setId(2);

        return Lists.newArrayList(queryHistory1, queryHistory2);
    }

    private QueryMetrics createQueryMetrics(long queryTime, long duration, boolean indexHit, String project) {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(duration);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setRealizations("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(indexHit);
        queryMetrics.setQueryTime(queryTime);
        queryMetrics.setQueryFirstDayOfMonth(TimeUtil.getMonthStart(queryTime));
        queryMetrics.setQueryFirstDayOfWeek(TimeUtil.getWeekStart(queryTime));
        queryMetrics.setQueryDay(TimeUtil.getDayStart(queryTime));
        queryMetrics.setProjectName(project);
        queryMetrics.setQueryStatus("SUCCEEDED");
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea");
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName(project);

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        queryMetrics.setRealizationMetrics(realizationMetricsList);
        return queryMetrics;
    }
}
