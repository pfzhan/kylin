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
package io.kyligence.kap.smart;

import static io.kyligence.kap.smart.model.GreedyModelTreesBuilderTest.smartUtHook;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AutoTestOnLearnKylinData;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class ReproposeTest extends AutoTestOnLearnKylinData {

    @Test
    public void testProposeOnReusableModel() {

        String sql = "select item_count, lstg_format_name, sum(price)\n" //
                + "from kylin_sales\n" //
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10;";
        // init a reusable model and build indexes
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        val modelContext = smartMaster.getContext().getModelContexts().get(0);
        val allLayouts = modelContext.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts.size());
        val initialLayout = allLayouts.get(0);
        Assert.assertEquals("[1, 3, 100000, 100001]", initialLayout.getColOrder().toString());

        // 1. case1: the layout is the best, will not propose another layout
        String sql1 = "select item_count, lstg_format_name, sum(price)\n" //
                + "from kylin_sales where item_count > 0\n" //
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10;";
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, new String[] { sql1 });
        SmartMaster smartMaster1 = new SmartMaster(context1);
        smartMaster1.runUtWithContext(smartUtHook);
        val modelContext1 = smartMaster1.getContext().getModelContexts().get(0);
        val allLayouts1 = modelContext1.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts1.size());
        val layout1 = allLayouts1.get(0);
        Assert.assertEquals("[1, 3, 100000, 100001]", layout1.getColOrder().toString());

        // 2. case2: the layout used is not the best, propose another layout
        String sql2 = "select item_count, sum(price)\n" //
                + "from kylin_sales where item_count > 0\n" //
                + "group by item_count\n" //
                + "order by item_count\n" //
                + "limit 10;";
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, new String[] { sql2 });
        SmartMaster smartMaster2 = new SmartMaster(context2);
        smartMaster2.runUtWithContext(smartUtHook);
        val modelContext2 = smartMaster2.getContext().getModelContexts().get(0);
        val allLayouts2 = modelContext2.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(2, allLayouts2.size());
        val layout20 = allLayouts2.get(0);
        val layout21 = allLayouts2.get(1);
        Assert.assertEquals("[1, 3, 100000, 100001]", layout20.getColOrder().toString());
        Assert.assertEquals("[1, 100000, 100001]", layout21.getColOrder().toString());
    }

    @Test
    public void testReproposeChangedByTableStats() {

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), proj);

        // 1. initial propose
        String sql = "select seller_id, lstg_format_name, count(1), sum(price)\n" //
                + "from kylin_sales\n" //
                + "where seller_id = 10000002 or lstg_format_name = 'FP-non GTC'\n" //
                + "group by seller_id, lstg_format_name\n" //
                + "order by seller_id, lstg_format_name\n" //
                + "limit 20";
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        val modelContext = smartMaster.getContext().getModelContexts().get(0);
        val allLayouts = modelContext.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts.size());
        val initialLayout = allLayouts.get(0);
        Assert.assertEquals("[9, 3, 100000, 100001]", initialLayout.getColOrder().toString());

        // 2. mock a sampling result
        String tableIdentity = "DEFAULT.KYLIN_SALES";
        val tableDesc = tableMgr.getTableDesc(tableIdentity);
        final TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);
        TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
        tableExt.setIdentity(tableIdentity);
        val col1 = tableExt.getColumnStatsByName("LSTG_FORMAT_NAME");
        col1.setCardinality(10000);
        col1.setTableExtDesc(tableExt);
        val col2 = tableExt.getColumnStatsByName("SELLER_ID");
        col2.setCardinality(100);
        col2.setTableExtDesc(tableExt);
        tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);

        // 3. re-propose with table stats
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, new String[] { sql });
        SmartMaster smartMaster1 = new SmartMaster(context1);
        smartMaster1.runUtWithContext(smartUtHook);
        val modelContexts1 = smartMaster1.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts1.size());
        val modelContext1 = modelContexts1.get(0);
        val layouts = modelContext1.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(2, layouts.size());
        val layout10 = layouts.get(0);
        val layout11 = layouts.get(1);
        Assert.assertEquals("[9, 3, 100000, 100001]", layout10.getColOrder().toString());
        Assert.assertEquals("[3, 9, 100000, 100001]", layout11.getColOrder().toString());
    }

    @Test
    public void testProposedLayoutConsistency() {
        List<String> sqlList = Lists.newArrayList("select test_kylin_fact.order_id, lstg_format_name\n"
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id\n"
                + "order by test_kylin_fact.order_id, lstg_format_name\n",
                "select lstg_format_name, seller_id, sum(price)\n"
                        + "from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id\n"
                        + "group by lstg_format_name, seller_id\n" //
                        + "order by lstg_format_name, seller_id\n",
                "select lstg_format_name, sum(price)\n"
                        + "from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id\n"
                        + "inner join test_country on test_account.account_country = test_country.country\n"
                        + "group by lstg_format_name\n" //
                        + "order by lstg_format_name\n",
                "select lstg_format_name, test_kylin_fact.leaf_categ_id, sum(price)\n"
                        + "from test_kylin_fact inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"
                        + "group by lstg_format_name, test_kylin_fact.leaf_categ_id\n"
                        + "order by lstg_format_name, test_kylin_fact.leaf_categ_id\n",
                "select account_country\n"
                        + "from test_account inner join test_country on test_account.account_country = test_country.country\n"
                        + "order by account_country\n" //
        );
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                sqlList.toArray(new String[0]));
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        final Map<String, AccelerateInfo> secondRoundMap = smartMaster.getContext().getAccelerateInfoMap();
        final Set<String> secondRoundProposedColOrders = collectColOrders(secondRoundMap.values());

        // Suggested layouts should be independent of the order of input sqls
        {
            Collections.shuffle(sqlList); // shuffle and propose
            val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                    sqlList.toArray(new String[0]));
            smartMaster = new SmartMaster(context1);
            smartMaster.runUtWithContext(smartUtHook);
            final Map<String, AccelerateInfo> thirdRoundMap = smartMaster.getContext().getAccelerateInfoMap();
            final Set<String> thirdRoundProposedColOrders = collectColOrders(thirdRoundMap.values());
            Assert.assertEquals(secondRoundProposedColOrders, thirdRoundProposedColOrders);
        }

        // Suggested layouts should be independent of modeling by a single batch or multi-batches
        {
            List<String> batchOne = sqlList.subList(0, sqlList.size() / 2);
            val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                    batchOne.toArray(new String[0]));
            smartMaster = new SmartMaster(context1);
            smartMaster.runUtWithContext(smartUtHook);
            final Map<String, AccelerateInfo> batchOneMap = smartMaster.getContext().getAccelerateInfoMap();

            List<String> batchTwo = sqlList.subList(sqlList.size() / 2, sqlList.size());
            val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                    batchTwo.toArray(new String[0]));
            smartMaster = new SmartMaster(context2);
            smartMaster.runUtWithContext(smartUtHook);
            final Map<String, AccelerateInfo> batchTwoMap = smartMaster.getContext().getAccelerateInfoMap();

            Set<String> batchProposedColOrders = Sets.newHashSet();
            batchProposedColOrders.addAll(collectColOrders(batchOneMap.values()));
            batchProposedColOrders.addAll(collectColOrders(batchTwoMap.values()));
            Assert.assertEquals(secondRoundProposedColOrders, batchProposedColOrders);
        }
    }

    private Set<String> collectColOrders(Collection<AccelerateInfo> accelerateInfoList) {
        Set<String> allProposedColOrder = Sets.newHashSet();
        accelerateInfoList.forEach(accelerateInfo -> {
            final Set<AccelerateInfo.QueryLayoutRelation> layouts = accelerateInfo.getRelatedLayouts();
            Set<String> colOrders = collectAllColOrders(getTestConfig(), getProject(), layouts);
            allProposedColOrder.addAll(colOrders);
        });
        return allProposedColOrder;
    }

    private static Set<String> collectAllColOrders(KylinConfig kylinConfig, String project,
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts) {
        Set<String> sets = Sets.newHashSet();
        if (CollectionUtils.isEmpty(relatedLayouts)) {
            return sets;
        }

        relatedLayouts.forEach(layoutRelation -> {
            final List<String> colOrderNames = findColOrderNames(kylinConfig, project, layoutRelation);
            sets.add(String.join(",", colOrderNames));
        });

        return sets;
    }

    private static List<String> findColOrderNames(KylinConfig kylinConfig, String project,
            AccelerateInfo.QueryLayoutRelation queryLayoutRelation) {
        List<String> colOrderNames = Lists.newArrayList();

        final IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project)
                .getIndexPlan(queryLayoutRelation.getModelId());
        val layout = indexPlan.getLayoutEntity(queryLayoutRelation.getLayoutId());
        ImmutableList<Integer> colOrder = layout.getColOrder();
        BiMap<Integer, TblColRef> effectiveDimCols = layout.getIndex().getEffectiveDimCols();
        ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures = layout.getIndex().getEffectiveMeasures();
        colOrder.forEach(column -> {
            if (column < NDataModel.MEASURE_ID_BASE) {
                colOrderNames.add(effectiveDimCols.get(column).getName());
            } else {
                colOrderNames.add(effectiveMeasures.get(column).getName());
            }
        });
        return colOrderNames;
    }

    private String getProject() {
        return "newten";
    }

}
