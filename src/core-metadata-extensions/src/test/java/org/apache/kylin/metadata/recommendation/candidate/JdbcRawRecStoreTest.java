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

package org.apache.kylin.metadata.recommendation.candidate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItemMapper;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
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

        RawRecItemMapper mapper = jdbcRawRecStore.getSqlSessionTemplate().getMapper(RawRecItemMapper.class);
        InsertStatementProvider<RawRecItem> insertStatement = jdbcRawRecStore.getInsertProvider(recItem, false);
        mapper.insert(insertStatement);
        mapper.selectOne(jdbcRawRecStore.getSelectByIdStatementProvider(recItem.getId()));
    }

    @Test
    public void testSaveAndUpdate() {
        // create and save CC rec item
        RawRecItem recItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem.setState(RawRecItem.RawRecState.INITIAL);
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
        recItem.setUniqueFlag("innerExp1");
        jdbcRawRecStore.save(recItem);
        assertEquals(1, recItem.getId());

        // save CC rec item again
        ccRecItemV2.setCreateTime(1000);
        recItem.setCreateTime(10000L);
        recItem.setSemanticVersion(2);
        recItem.setUniqueFlag("innerExp2");
        jdbcRawRecStore.save(recItem);
        assertEquals(2, recItem.getId());
        recItem.setCreateTime(20000L);
        recItem.setSemanticVersion(1);
        recItem.setUniqueFlag("innerExp3");
        jdbcRawRecStore.save(recItem);
        assertEquals(3, recItem.getId());

        // create and save dimension rec item
        RawRecItem dimRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.DIMENSION);
        DimensionRecItemV2 dimensionRecItemV2 = new DimensionRecItemV2();
        dimRecItem.setState(RawRecItem.RawRecState.INITIAL);
        dimRecItem.setRecEntity(dimensionRecItemV2);
        dimRecItem.setDependIDs(new int[] { 1 });
        dimRecItem.setUniqueFlag("d__TABLE$1");
        jdbcRawRecStore.save(dimRecItem);
        assertEquals(4, dimRecItem.getId());

        // create and save measure rec item
        RawRecItem measureRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.MEASURE);
        MeasureRecItemV2 measureRecItemV2 = new MeasureRecItemV2();
        measureRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        measureRawRecItem.setDependIDs(new int[] { 1 });
        measureRawRecItem.setRecEntity(measureRecItemV2);
        measureRawRecItem.setUniqueFlag("m__TABLE$1");
        jdbcRawRecStore.save(measureRawRecItem);
        assertEquals(5, measureRawRecItem.getId());

        // create and save layout rec item
        RawRecItem layoutRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        LayoutRecItemV2 layoutRecItemV2 = new LayoutRecItemV2();
        layoutRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        layoutRawRecItem.setDependIDs(new int[] { -4, -5 });
        layoutRawRecItem.setRecEntity(layoutRecItemV2);
        layoutRawRecItem.setUniqueFlag("z7a0c38e-0fb0-480c-80e1-03039364991f");
        jdbcRawRecStore.save(layoutRawRecItem);
        assertEquals(6, layoutRawRecItem.getId());

        // update
        final List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("test", "abc", 1, 10);
        rawRecItems.forEach(item -> item.setCost(item.getId()));
        jdbcRawRecStore.batchAddOrUpdate(rawRecItems);

        // validate
        assertEquals(5, rawRecItems.size());
        assertEquals(1, jdbcRawRecStore.listAll("test", "abc", 2, 10).size());
        assertEquals(6, jdbcRawRecStore.listAll("test", "abc", 10).size());
        assertEquals(1, rawRecItems.get(0).getId());
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(0).getType());
        assertEquals(1.0, rawRecItems.get(0).getCost(), 0.1);

        assertEquals(3, rawRecItems.get(1).getId());
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(1).getType());
        assertEquals(3.0, rawRecItems.get(1).getCost(), 0.1);

        assertEquals(4, rawRecItems.get(2).getId());
        assertEquals(RawRecItem.RawRecType.DIMENSION, rawRecItems.get(2).getType());
        assertEquals(4.0, rawRecItems.get(2).getCost(), 0.1);

        assertEquals(5, rawRecItems.get(3).getId());
        assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(3).getType());
        assertEquals(5.0, rawRecItems.get(3).getCost(), 0.1);

        assertEquals(6, rawRecItems.get(4).getId());
        assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, rawRecItems.get(4).getType());
        assertEquals(6.0, rawRecItems.get(4).getCost(), 0.1);
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
        assertEquals(2, allRawRecItems.size());

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
        assertEquals(4, jdbcRawRecStore.queryAll().size());

        // create and save dimension rec item
        RawRecItem dimRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.DIMENSION);
        DimensionRecItemV2 dimensionRecItemV2 = new DimensionRecItemV2();
        dimRecItem.setState(RawRecItem.RawRecState.INITIAL);
        dimRecItem.setRecEntity(dimensionRecItemV2);
        dimRecItem.setDependIDs(new int[] { 1 });
        dimRecItem.setUniqueFlag("d__TABLE$1");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(dimRecItem));
        assertEquals(5, dimRecItem.getId());

        // create and save measure rec item
        RawRecItem measureRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.MEASURE);
        MeasureRecItemV2 measureRecItemV2 = new MeasureRecItemV2();
        measureRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        measureRawRecItem.setDependIDs(new int[] { 1 });
        measureRawRecItem.setRecEntity(measureRecItemV2);
        measureRawRecItem.setUniqueFlag("m__TABLE$1");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(measureRawRecItem));
        assertEquals(6, measureRawRecItem.getId());

        // create and save layout rec item
        RawRecItem layoutRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        LayoutRecItemV2 layoutRecItemV2 = new LayoutRecItemV2();
        layoutRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        layoutRawRecItem.setDependIDs(new int[] { -4, -5 });
        layoutRawRecItem.setRecEntity(layoutRecItemV2);
        layoutRawRecItem.setUniqueFlag("z7a0c38e-0fb0-480c-80e1-03039364991f");
        jdbcRawRecStore.batchAddOrUpdate(Lists.newArrayList(layoutRawRecItem));
        assertEquals(7, layoutRawRecItem.getId());

        // update
        allRawRecItems = jdbcRawRecStore.queryAll();
        allRawRecItems.forEach(item -> item.setCost(item.getId()));
        jdbcRawRecStore.batchAddOrUpdate(allRawRecItems);

        // validate
        allRawRecItems = jdbcRawRecStore.queryAll();
        assertEquals(1, allRawRecItems.get(0).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(0).getType());
        assertEquals(1, allRawRecItems.get(0).getCost(), 0.1);

        assertEquals(2, allRawRecItems.get(1).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(1).getType());
        assertEquals(2, allRawRecItems.get(1).getCost(), 0.1);

        assertEquals(3, allRawRecItems.get(2).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(2).getType());
        assertEquals(3, allRawRecItems.get(2).getCost(), 0.1);

        assertEquals(4, allRawRecItems.get(3).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, allRawRecItems.get(3).getType());
        assertEquals(4, allRawRecItems.get(3).getCost(), 0.1);

        assertEquals(5, allRawRecItems.get(4).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.DIMENSION, allRawRecItems.get(4).getType());
        assertEquals(5, allRawRecItems.get(4).getCost(), 0.1);

        assertEquals(6, allRawRecItems.get(5).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.MEASURE, allRawRecItems.get(5).getType());
        assertEquals(6, allRawRecItems.get(5).getCost(), 0.1);

        assertEquals(7, allRawRecItems.get(6).getId(), 0.1);
        assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, allRawRecItems.get(6).getType());
        assertEquals(7, allRawRecItems.get(6).getCost(), 0.1);

        // test semantic version of model not exist
        RawRecItem recItem = jdbcRawRecStore.queryById(7);
        recItem.setState(RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recItem);
        List<RawRecItem> additionalLayoutRecItems = jdbcRawRecStore.queryNonAppliedLayoutRecItems("test", "abc", false);
        Assert.assertTrue(additionalLayoutRecItems.isEmpty());
    }

    @Test
    public void testSaveAndUpdateWithCheck() {
        // create and save layout rec item
        RawRecItem layoutRawRecItem = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
        LayoutRecItemV2 layoutRecItemV2 = new LayoutRecItemV2();
        LayoutEntity layoutEntity = new LayoutEntity();
        layoutEntity.setColOrder(Arrays.asList(1, 2));
        layoutRecItemV2.setLayout(layoutEntity);
        layoutRawRecItem.setState(RawRecItem.RawRecState.INITIAL);
        layoutRawRecItem.setDependIDs(new int[] { -4, -5 });
        layoutRawRecItem.setRecEntity(layoutRecItemV2);

        String content = RawRecUtil.getContent(layoutRawRecItem);
        String md5 = RawRecUtil.computeMD5(content);

        layoutRawRecItem.setUniqueFlag(md5);
        jdbcRawRecStore.save(layoutRawRecItem);
        assertEquals(1, layoutRawRecItem.getId());

        layoutRecItemV2.getLayout().setColOrder(Arrays.asList(1, 2, 3, 4));
        layoutRawRecItem.setUniqueFlag(md5 + "_1");
        jdbcRawRecStore.save(layoutRawRecItem);
        assertEquals(2, layoutRawRecItem.getId());

        layoutRecItemV2.getLayout().setColOrder(Arrays.asList(1, 4));
        layoutRawRecItem.setUniqueFlag(md5 + "_2");
        jdbcRawRecStore.save(layoutRawRecItem);
        assertEquals(3, layoutRawRecItem.getId());

        Map<String, RawRecItem> layoutUniqueFlagRecMap = new HashMap<>();
        jdbcRawRecStore.queryAll().forEach(recItem -> layoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));
        Map<String, List<String>> md5ToFlags = RawRecUtil.uniqueFlagsToMd5Map(layoutUniqueFlagRecMap.keySet());

        String newContent = RawRecUtil.getContent(layoutRawRecItem);
        Pair<String, RawRecItem> recItemPair = jdbcRawRecStore.queryRecItemByMd5(md5, newContent);
        assertEquals(md5 + "_2", recItemPair.getFirst());
        assertNotNull(recItemPair.getSecond());
        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, newContent, md5ToFlags, layoutUniqueFlagRecMap);
        assertEquals(md5 + "_2", recItemPair.getFirst());
        assertNotNull(recItemPair.getSecond());

        layoutRecItemV2.getLayout().setColOrder(Arrays.asList(1, 2, 3));
        newContent = RawRecUtil.getContent(layoutRawRecItem);
        recItemPair = jdbcRawRecStore.queryRecItemByMd5(md5, newContent);
        assertEquals(md5 + "_3", recItemPair.getFirst());
        assertNull(recItemPair.getSecond());
        recItemPair = RawRecUtil.getRawRecItemFromMap(md5, newContent, md5ToFlags, layoutUniqueFlagRecMap);
        assertEquals(md5 + "_3", recItemPair.getFirst());
        assertNull(recItemPair.getSecond());

        String fakeMd5 = RawRecUtil.computeMD5(newContent);
        recItemPair = jdbcRawRecStore.queryRecItemByMd5(fakeMd5, newContent);
        assertEquals(recItemPair.getFirst(), fakeMd5);
        assertNull(recItemPair.getSecond());
        recItemPair = RawRecUtil.getRawRecItemFromMap(fakeMd5, newContent, md5ToFlags, layoutUniqueFlagRecMap);
        assertEquals(recItemPair.getFirst(), fakeMd5);
        assertNull(recItemPair.getSecond());
    }

    private void prepare() {
        // prepare
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        RawRecItem recItem1 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem1.setState(RawRecItem.RawRecState.INITIAL);
        recItem1.setUniqueFlag("innerExp1");
        recItem1.setRecEntity(ccRecItemV2);
        recItem1.setDependIDs(new int[] { 0 });
        RawRecItem recItem2 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem2.setState(RawRecItem.RawRecState.INITIAL);
        recItem2.setUniqueFlag("innerExp2");
        recItem2.setRecEntity(ccRecItemV2);
        recItem2.setDependIDs(new int[] { 0 });
        RawRecItem recItem3 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem3.setState(RawRecItem.RawRecState.INITIAL);
        recItem3.setUniqueFlag("innerExp3");
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
        assertEquals(3, jdbcRawRecStore.queryAll().size());

        // after delete
        jdbcRawRecStore.deleteByProject(TO_BE_DELETE);
        assertEquals(1, jdbcRawRecStore.queryAll().size());
        assertEquals("other", jdbcRawRecStore.queryAll().get(0).getProject());
    }

    @Test
    public void testDeleteAll() {
        prepare();
        assertEquals(3, jdbcRawRecStore.queryAll().size());
        jdbcRawRecStore.deleteAll();
        assertEquals(0, jdbcRawRecStore.queryAll().size());
    }

    @Test
    public void testCleanForDeletedProject() {
        // prepare
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        RawRecItem recItem1 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem1.setState(RawRecItem.RawRecState.INITIAL);
        recItem1.setUniqueFlag("innerExp1");
        recItem1.setRecEntity(ccRecItemV2);
        recItem1.setDependIDs(new int[] { 0 });
        RawRecItem recItem2 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem2.setState(RawRecItem.RawRecState.INITIAL);
        recItem2.setUniqueFlag("innerExp2");
        recItem2.setRecEntity(ccRecItemV2);
        recItem2.setDependIDs(new int[] { 0 });
        RawRecItem recItem3 = new RawRecItem("test", "abc", 1, RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem3.setState(RawRecItem.RawRecState.INITIAL);
        recItem3.setUniqueFlag("innerExp3");
        recItem3.setRecEntity(ccRecItemV2);
        recItem3.setDependIDs(new int[] { 0 });
        recItem1.setProject(TO_BE_DELETE);
        recItem2.setProject(TO_BE_DELETE);
        recItem3.setProject("other");
        jdbcRawRecStore.save(recItem1);
        jdbcRawRecStore.save(recItem2);
        jdbcRawRecStore.save(recItem3);

        // before delete
        assertEquals(3, jdbcRawRecStore.queryAll().size());

        // after delete
        jdbcRawRecStore.cleanForDeletedProject(Lists.newArrayList("other"));
        assertEquals(1, jdbcRawRecStore.queryAll().size());
        assertEquals("other", jdbcRawRecStore.queryAll().get(0).getProject());
    }

}
