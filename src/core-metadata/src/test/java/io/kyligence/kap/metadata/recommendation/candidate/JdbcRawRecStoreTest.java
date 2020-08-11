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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.lang.reflect.Field;
import java.util.List;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRawRecStoreTest extends NLocalFileMetadataTestCase {

    private final String TO_BE_DELETE = "to_be_delete_project";

    private JdbcRawRecStore jdbcRawRecStore;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @After
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();

        log.debug("clean SqlSessionFactory...");
        Class<RawRecStoreUtil> clazz = RawRecStoreUtil.class;
        Field sqlSessionFactory = clazz.getDeclaredField("sqlSessionFactory");
        sqlSessionFactory.setAccessible(true);
        sqlSessionFactory.set(null, null);
        System.out.println(sqlSessionFactory.get(null));
        sqlSessionFactory.setAccessible(false);
        log.debug("clean SqlSessionFactory success");
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
            InsertStatementProvider<RawRecItem> insertStatement = jdbcRawRecStore.getInsertProvider(recItem);
            mapper.insert(insertStatement);
            mapper.selectOne(jdbcRawRecStore.getSelectByIdStatementProvider(recItem.getId()));
        }
    }

    @Test
    public void testSave() {
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

        jdbcRawRecStore.save(recItem);
        Assert.assertEquals(1, recItem.getId());
        ccRecItemV2.setCreateTime(1000);
        recItem.setCreateTime(10000L);
        recItem.setSemanticVersion(2);
        jdbcRawRecStore.save(recItem);
        Assert.assertEquals(2, recItem.getId());

        recItem.setCost(recItem.getId());
        final List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("test", "abc", 1, 10);
        rawRecItems.forEach(item -> item.setCost(item.getId()));
        jdbcRawRecStore.update(rawRecItems);
    }

    @Test
    public void testDeleteByProject() {
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
        jdbcRawRecStore.deleteByProject(TO_BE_DELETE);
        Assert.assertEquals(1, jdbcRawRecStore.queryAll().size());
        Assert.assertEquals("other", jdbcRawRecStore.queryAll().get(0).getProject());
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

    @Test
    public void testBatchSave() {

    }

    @Test
    public void testSelectOne() {

    }

    @Test
    public void selectMany() {

    }
}
