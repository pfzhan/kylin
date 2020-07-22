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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;

public class NBasicSemiV2Test extends SemiAutoTestBase {

    private static final long QUERY_TIME = 1595520000000L;

    private JdbcRawRecStore jdbcRawRecStore;
    RawRecService rawRecommendation;

    @Before
    public void setup() throws Exception {
        super.setup();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        rawRecommendation = new RawRecService();
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
        smartMaster.runWithContext();

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

        String key = "SUM__TEST_KYLIN_FACT$9";
        Assert.assertTrue(measureRecItemMap.containsKey(key));
        measureRecItemMap.remove(key);
        final String measureName = measureRecItemMap.keySet().iterator().next();
        final String oneCCUuid = measureName.split("__")[1];
        Assert.assertTrue(ccRecItemMap.containsKey(oneCCUuid));
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE",
                ccRecItemMap.get(oneCCUuid).getCc().getExpression());
    }

    @Test
    public void testGenerateRawRecommendationsLayoutMetric() {
        // prepare two origin model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact", "select name from test_country" });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runWithContext();
        List<NDataModel> originModels = smartContext.getOriginModels();

        // generate raw recommendations for origin model
        rawRecommendation.generateRawRecommendations(getProject(), queryHistories());

        // check raw recommendations layoutMetric
        List<RawRecItem> countryRawRecItems = jdbcRawRecStore.listAll(getProject(), originModels.get(0).getUuid(), 0,
                10);
        Assert.assertEquals(2, countryRawRecItems.size());
        List<RawRecItem> factRawRecItems = jdbcRawRecStore.listAll(getProject(), originModels.get(1).getUuid(), 0, 10);
        Assert.assertEquals(2, factRawRecItems.size());

        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, jdbcRawRecStore.queryById(1).getType());
        Assert.assertEquals(RawRecItem.RawRecType.LAYOUT, jdbcRawRecStore.queryById(3).getType());
        Assert.assertEquals(1, jdbcRawRecStore.queryById(3).getLayoutMetric().getFrequencyMap().getDateFrequency()
                .get(QUERY_TIME).intValue());

        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, jdbcRawRecStore.queryById(2).getType());
        Assert.assertEquals(RawRecItem.RawRecType.LAYOUT, jdbcRawRecStore.queryById(4).getType());
        Assert.assertEquals(1, jdbcRawRecStore.queryById(4).getLayoutMetric().getFrequencyMap().getDateFrequency()
                .get(QUERY_TIME).intValue());
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select CAL_DT from test_kylin_fact");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setSql("select country from test_country");
        queryHistory2.setId(1);

        return Lists.newArrayList(queryHistory1, queryHistory2);
    }
}
