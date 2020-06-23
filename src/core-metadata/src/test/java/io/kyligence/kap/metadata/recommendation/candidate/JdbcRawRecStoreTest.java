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
import org.junit.Ignore;
import org.junit.Test;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;

public class JdbcRawRecStoreTest extends NLocalFileMetadataTestCase {

    private JdbcRawRecStore jdbcRawRecStore;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void destroy() {
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
            InsertStatementProvider<RawRecItem> insertStatement = jdbcRawRecStore.getInsertProvider(recItem);
            mapper.insert(insertStatement);
            mapper.selectOne(jdbcRawRecStore.getSelectByIdStatementProvider(recItem.getId()));
        }
    }

    @Ignore
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
    public void testBatchSave() {

    }

    @Test
    public void testSelectOne() {

    }

    @Test
    public void selectMany() {

    }
}
