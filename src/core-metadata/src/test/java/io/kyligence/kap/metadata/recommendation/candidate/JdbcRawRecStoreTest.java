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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRawRecStoreTest extends NLocalFileMetadataTestCase {

    private final String TO_BE_DELETE = "to_be_delete_project";

    private JdbcRawRecStore jdbcRawRecStore;
    private JdbcTemplate jdbcTemplate;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void destroy() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        cleanupTestMetadata();
    }

    @Test
    public void basicTest() {
        RawRecItem recItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem.setState(RawRecItem.RawRecState.INITIAL);
        recItem.setUniqueFlag("innerExp");
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        ComputedColumnDesc cc = new ComputedColumnDesc();
        cc.setColumnName("cc_auto_1");
        cc.setExpression("table1.col1 + table1.col2");
        cc.setInnerExpression("table1.col1 + table2.col2");
        cc.setDatatype("double");
        cc.setTableIdentity("ssb.table1");
        cc.setTableAlias("table1");
        cc.setUuid("random-uuid");
        ccRecItemV2.setCc(cc);
        ccRecItemV2.setCreateTime(100);
        recItem.setRecEntity(ccRecItemV2);
        recItem.setCreateTime(ccRecItemV2.getCreateTime());
        recItem.setDependIDs(new int[] { 1, 2 });

        SqlSessionFactory sqlSessionFactory = jdbcRawRecStore.getSqlSessionFactory();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            InsertStatementProvider<RawRecItem> insertStatement = jdbcRawRecStore.getInsertProvider(recItem, false);
            mapper.insert(insertStatement);
            mapper.selectOne(jdbcRawRecStore.getSelectByIdStatementProvider(recItem.getId()));
        }
    }

    @Test
    public void testSaveAndUpdate() {
        // create and save CC rec item
        RawRecItem recItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem.setState(RawRecItem.RawRecState.INITIAL);
        recItem.setUniqueFlag("2 * TEST_KYLIN_FACT.PRICE");
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        ComputedColumnDesc cc = new ComputedColumnDesc();
        cc.setColumnName("cc_auto_1");
        cc.setExpression("table1.col1 + table1.col2");
        cc.setInnerExpression("table1.col1 + table2.col2");
        cc.setDatatype("double");
        cc.setTableIdentity("ssb.table1");
        cc.setTableAlias("table1");
        cc.setUuid("random-uuid");
        ccRecItemV2.setCc(cc);
        ccRecItemV2.setCreateTime(100);
        recItem.setRecEntity(ccRecItemV2);
        recItem.setCreateTime(ccRecItemV2.getCreateTime());
        recItem.setDependIDs(new int[] { 1, 2 });
        jdbcRawRecStore.save(recItem);
        Assert.assertEquals(1, recItem.getId());

        // save CC rec item again
        ccRecItemV2.setCreateTime(1000);
        recItem.setCreateTime(10000L);
        recItem.setSemanticVersion(2);
        jdbcRawRecStore.save(recItem);
        Assert.assertEquals(2, recItem.getId());
        recItem.setCreateTime(20000L);
        recItem.setSemanticVersion(1);
        jdbcRawRecStore.save(recItem);
        Assert.assertEquals(3, recItem.getId());

        // create and save dimension rec item
        RawRecItem dimRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.DIMENSION);
        DimensionRecItemV2 dimensionRecItemV2 = new DimensionRecItemV2();
        dimRecItem.setState(RawRecItem.RawRecState.INITIAL);
        dimRecItem.setRecEntity(dimensionRecItemV2);
        dimRecItem.setDependIDs(new int[] { 1 });
        dimRecItem.setUniqueFlag("d__TABLE$1");
        jdbcRawRecStore.save(dimRecItem);
        Assert.assertEquals(4, dimRecItem.getId());

        // create and save measure rec item
        RawRecItem measureRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.MEASURE);
        MeasureRecItemV2 measureRecItemV2 = new MeasureRecItemV2();
        measureRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        measureRawRecItem.setDependIDs(new int[] { 1 });
        measureRawRecItem.setRecEntity(measureRecItemV2);
        measureRawRecItem.setUniqueFlag("m__TABLE$1");
        jdbcRawRecStore.save(measureRawRecItem);
        Assert.assertEquals(5, measureRawRecItem.getId());

        // create and save layout rec item
        RawRecItem layoutRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        LayoutRecItemV2 layoutRecItemV2 = new LayoutRecItemV2();
        layoutRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        layoutRawRecItem.setDependIDs(new int[] { -4, -5 });
        layoutRawRecItem.setRecEntity(layoutRecItemV2);
        layoutRawRecItem.setUniqueFlag("z7a0c38e-0fb0-480c-80e1-03039364991f");
        jdbcRawRecStore.save(layoutRawRecItem);
        Assert.assertEquals(6, layoutRawRecItem.getId());

        // update
        final List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("test", "abc", 1, 10);
        rawRecItems.forEach(item -> item.setCost(item.getId()));
        jdbcRawRecStore.update(rawRecItems);

        // validate
        Assert.assertEquals(5, rawRecItems.size());
        Assert.assertEquals(1, jdbcRawRecStore.listAll("test", "abc", 2, 10).size());
        Assert.assertEquals(6, jdbcRawRecStore.listAll("test", "abc", 10).size());
        Assert.assertEquals(1, rawRecItems.get(0).getId());
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(0).getType());
        Assert.assertEquals(1.0, rawRecItems.get(0).getCost(), 0.1);

        Assert.assertEquals(3, rawRecItems.get(1).getId());
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(1).getType());
        Assert.assertEquals(3.0, rawRecItems.get(1).getCost(), 0.1);

        Assert.assertEquals(4, rawRecItems.get(2).getId());
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, rawRecItems.get(2).getType());
        Assert.assertEquals(4.0, rawRecItems.get(2).getCost(), 0.1);

        Assert.assertEquals(5, rawRecItems.get(3).getId());
        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(3).getType());
        Assert.assertEquals(5.0, rawRecItems.get(3).getCost(), 0.1);

        Assert.assertEquals(6, rawRecItems.get(4).getId());
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, rawRecItems.get(4).getType());
        Assert.assertEquals(6.0, rawRecItems.get(4).getCost(), 0.1);
    }

    @Test
    public void testBatchSaveAndUpdate() {
        // create and batch save CC rec item
        RawRecItem ccRawRecItem1 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        ccRawRecItem1.setRecEntity(ccRecItemV2);
        ccRawRecItem1.setState(RawRecItem.RawRecState.INITIAL);
        ccRawRecItem1.setUniqueFlag("2 * TEST_KYLIN_FACT.PRICE");
        ccRawRecItem1.setDependIDs(new int[] { 1 });
        RawRecItem ccRawRecItem2 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        ccRawRecItem2.setRecEntity(ccRecItemV2);
        ccRawRecItem2.setState(RawRecItem.RawRecState.INITIAL);
        ccRawRecItem2.setUniqueFlag("3 * TEST_KYLIN_FACT.PRICE");
        ccRawRecItem2.setDependIDs(new int[] { 1 });
        List<RawRecItem> recList = Lists.newArrayList(ccRawRecItem1, ccRawRecItem2);
        jdbcRawRecStore.batchAddOrUpdate(recList);
        List<RawRecItem> allRawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(2, allRawRecItems.size());

        // create and batch save CC rec item again
        RawRecItem ccRawRecItem3 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        ccRawRecItem3.setRecEntity(ccRecItemV2);
        ccRawRecItem3.setState(RawRecItem.RawRecState.INITIAL);
        ccRawRecItem3.setUniqueFlag("4 * TEST_KYLIN_FACT.PRICE");
        ccRawRecItem3.setDependIDs(new int[] { 1 });
        RawRecItem ccRawRecItem4 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        ccRawRecItem4.setRecEntity(ccRecItemV2);
        ccRawRecItem4.setState(RawRecItem.RawRecState.INITIAL);
        ccRawRecItem4.setUniqueFlag("5 * TEST_KYLIN_FACT.PRICE");
        ccRawRecItem4.setDependIDs(new int[] { 1 });
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(ccRawRecItem3, ccRawRecItem4));
        Assert.assertEquals(4, jdbcRawRecStore.queryAll().size());

        // create and save dimension rec item
        RawRecItem dimRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.DIMENSION);
        DimensionRecItemV2 dimensionRecItemV2 = new DimensionRecItemV2();
        dimRecItem.setState(RawRecItem.RawRecState.INITIAL);
        dimRecItem.setRecEntity(dimensionRecItemV2);
        dimRecItem.setDependIDs(new int[] { 1 });
        dimRecItem.setUniqueFlag("d__TABLE$1");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(dimRecItem));
        Assert.assertEquals(5, dimRecItem.getId());

        // create and save measure rec item
        RawRecItem measureRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.MEASURE);
        MeasureRecItemV2 measureRecItemV2 = new MeasureRecItemV2();
        measureRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        measureRawRecItem.setDependIDs(new int[] { 1 });
        measureRawRecItem.setRecEntity(measureRecItemV2);
        measureRawRecItem.setUniqueFlag("m__TABLE$1");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(measureRawRecItem));
        Assert.assertEquals(6, measureRawRecItem.getId());

        // create and save layout rec item
        RawRecItem layoutRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        LayoutRecItemV2 layoutRecItemV2 = new LayoutRecItemV2();
        layoutRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        layoutRawRecItem.setDependIDs(new int[] { -4, -5 });
        layoutRawRecItem.setRecEntity(layoutRecItemV2);
        layoutRawRecItem.setUniqueFlag("z7a0c38e-0fb0-480c-80e1-03039364991f");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(layoutRawRecItem));
        Assert.assertEquals(7, layoutRawRecItem.getId());

        // update
        allRawRecItems = jdbcRawRecStore.queryAll();
        allRawRecItems.forEach(item -> item.setCost(item.getId()));
        jdbcRawRecStore.batchAddOrUpdate(allRawRecItems);

        // validate
        allRawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(1, allRawRecItems.get(0).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(0).getType());
        Assert.assertEquals(1, allRawRecItems.get(0).getCost(), 0.1);

        Assert.assertEquals(2, allRawRecItems.get(1).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(1).getType());
        Assert.assertEquals(2, allRawRecItems.get(1).getCost(), 0.1);

        Assert.assertEquals(3, allRawRecItems.get(2).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(2).getType());
        Assert.assertEquals(3, allRawRecItems.get(2).getCost(), 0.1);

        Assert.assertEquals(4, allRawRecItems.get(3).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(3).getType());
        Assert.assertEquals(4, allRawRecItems.get(3).getCost(), 0.1);

        Assert.assertEquals(5, allRawRecItems.get(4).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, allRawRecItems.get(4).getType());
        Assert.assertEquals(5, allRawRecItems.get(4).getCost(), 0.1);

        Assert.assertEquals(6, allRawRecItems.get(5).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, allRawRecItems.get(5).getType());
        Assert.assertEquals(6, allRawRecItems.get(5).getCost(), 0.1);

        Assert.assertEquals(7, allRawRecItems.get(6).getId(), 0.1);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, allRawRecItems.get(6).getType());
        Assert.assertEquals(7, allRawRecItems.get(6).getCost(), 0.1);

        // test semantic version of model not exist
        RawRecItem recItem = jdbcRawRecStore.queryById(7);
        recItem.setState(RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recItem);
        List<RawRecItem> additionalLayoutRecItems = jdbcRawRecStore.queryNonAppliedLayoutRecItems("test", "abc", false);
        Assert.assertTrue(additionalLayoutRecItems.isEmpty());
    }

    private void prepare() {
        // prepare
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        RawRecItem recItem1 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem1.setState(RawRecItem.RawRecState.INITIAL);
        recItem1.setUniqueFlag("innerExp");
        recItem1.setRecEntity(ccRecItemV2);
        recItem1.setDependIDs(new int[] { 0 });
        RawRecItem recItem2 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem2.setState(RawRecItem.RawRecState.INITIAL);
        recItem2.setUniqueFlag("innerExp");
        recItem2.setRecEntity(ccRecItemV2);
        recItem2.setDependIDs(new int[] { 0 });
        RawRecItem recItem3 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem3.setState(RawRecItem.RawRecState.INITIAL);
        recItem3.setUniqueFlag("innerExp");
        recItem3.setRecEntity(ccRecItemV2);
        recItem3.setDependIDs(new int[] { 0 });
        recItem1.setProject(TO_BE_DELETE);
        recItem2.setProject(TO_BE_DELETE);
        recItem3.setProject("other");
        jdbcRawRecStore.save(recItem1);
        jdbcRawRecStore.save(recItem2);
        jdbcRawRecStore.save(recItem3);
    }

    @Test
    public void testDeleteByProject() {
        prepare();

        // before delete
        Assert.assertEquals(3, jdbcRawRecStore.queryAll().size());

        // after delete
        jdbcRawRecStore.deleteByProject(TO_BE_DELETE);
        Assert.assertEquals(1, jdbcRawRecStore.queryAll().size());
        Assert.assertEquals("other", jdbcRawRecStore.queryAll().get(0).getProject());
    }

    @Test
    public void testDeleteAll() {
        prepare();
        Assert.assertEquals(3, jdbcRawRecStore.queryAll().size());
        jdbcRawRecStore.deleteAll();
        Assert.assertEquals(0, jdbcRawRecStore.queryAll().size());
    }

    @Test
    public void testCleanForDeletedProject() {
        // prepare
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        RawRecItem recItem1 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem1.setState(RawRecItem.RawRecState.INITIAL);
        recItem1.setUniqueFlag("innerExp");
        recItem1.setRecEntity(ccRecItemV2);
        recItem1.setDependIDs(new int[] { 0 });
        RawRecItem recItem2 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem2.setState(RawRecItem.RawRecState.INITIAL);
        recItem2.setUniqueFlag("innerExp");
        recItem2.setRecEntity(ccRecItemV2);
        recItem2.setDependIDs(new int[] { 0 });
        RawRecItem recItem3 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem3.setState(RawRecItem.RawRecState.INITIAL);
        recItem3.setUniqueFlag("innerExp");
        recItem3.setRecEntity(ccRecItemV2);
        recItem3.setDependIDs(new int[] { 0 });
        recItem1.setProject(TO_BE_DELETE);
        recItem2.setProject(TO_BE_DELETE);
        recItem3.setProject("other");
        jdbcRawRecStore.save(recItem1);
        jdbcRawRecStore.save(recItem2);
        jdbcRawRecStore.save(recItem3);

        // before delete
        Assert.assertEquals(3, jdbcRawRecStore.queryAll().size());

        // after delete
        jdbcRawRecStore.cleanForDeletedProject(Lists.newArrayList("other"));
        Assert.assertEquals(1, jdbcRawRecStore.queryAll().size());
        Assert.assertEquals("other", jdbcRawRecStore.queryAll().get(0).getProject());
    }

}
