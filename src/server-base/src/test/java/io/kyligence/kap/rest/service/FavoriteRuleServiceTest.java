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

package io.kyligence.kap.rest.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.rest.response.ImportSqlResponse;
import lombok.val;
import lombok.var;

public class FavoriteRuleServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    @InjectMocks
    private FavoriteRuleService favoriteRuleService = Mockito.spy(new FavoriteRuleService());

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setUp() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(favoriteRuleService, "aclEvaluate", aclEvaluate);
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBlacklistBasics() {
        final String sqlPattern1 = "test sql pattern 1";
        final String sqlPattern2 = "test sql pattern 2";
        final String sqlPattern3 = "test sql pattern 3";
        final String sqlPattern4 = "test sql pattern 4";

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery(sqlPattern1);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery2 = new FavoriteQuery(sqlPattern2);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        FavoriteQuery favoriteQuery3 = new FavoriteQuery(sqlPattern3);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        FavoriteQuery favoriteQuery4 = new FavoriteQuery(sqlPattern4);
        favoriteQuery4.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);

        favoriteQueryManager.create(new HashSet() {
            {
                add(favoriteQuery1);
                add(favoriteQuery2);
                add(favoriteQuery3);
                add(favoriteQuery4);
            }
        });
        List<FavoriteQuery> favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(4, favoriteQueries.size());
        List<FavoriteRule.SQLCondition> blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        Assert.assertEquals(1, blacklistSqls.size());

        // append sql pattern1 to blacklist by batch
        var fqUuids = Lists.newArrayList(favoriteQueryManager.get(sqlPattern1).getUuid(),
                favoriteQueryManager.get(sqlPattern2).getUuid());
        favoriteRuleService.batchDeleteFQs(PROJECT, fqUuids, true);
        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        Assert.assertEquals(3, blacklistSqls.size());
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(2, favoriteQueries.size());

        // delete fq by batch
        fqUuids = Lists.newArrayList(favoriteQueryManager.get(sqlPattern3).getUuid(),
                favoriteQueryManager.get(sqlPattern4).getUuid());
        favoriteRuleService.batchDeleteFQs(PROJECT, fqUuids, false);
        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        Assert.assertEquals(3, blacklistSqls.size());
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(0, favoriteQueries.size());

        // create fq whose sql pattern is in blacklist
        FavoriteQuery sqlPattern1FQ = new FavoriteQuery(sqlPattern1);
        sqlPattern1FQ.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        favoriteQueryManager.create(new HashSet() {
            {
                add(sqlPattern1FQ);
            }
        });
        Assert.assertEquals(0, favoriteQueryManager.getAll().size());
        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(3, blacklistSqls.size());
        Assert.assertEquals(0, favoriteQueries.size());

        // delete not exist favorite query
        try {
            favoriteRuleService.batchDeleteFQs(PROJECT, Lists.newArrayList("not_exist_uuid", "not_exist_uuid2"), false);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertEquals("Favorite query 'not_exist_uuid' does not exist", ex.getMessage());
        }

        // returned blacklist sql is sorted by create time
        FavoriteRule.SQLCondition sqlCondition1 = blacklistSqls.get(0);
        FavoriteRule.SQLCondition sqlCondition2 = blacklistSqls.get(1);
        Assert.assertTrue(sqlCondition1.getCreateTime() >= sqlCondition2.getCreateTime());

        // test filter blacklist by sql
        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "sql\n pattern\t 1");
        Assert.assertEquals(1, blacklistSqls.size());

        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "not_exist_sql");
        Assert.assertEquals(0, blacklistSqls.size());

        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        Assert.assertEquals(3, blacklistSqls.size());

        // remove sql pattern from blacklist
        favoriteRuleService.removeBlacklistSql(blacklistSqls.get(0).getId(), PROJECT);
        blacklistSqls = favoriteRuleService.getBlacklistSqls(PROJECT, "");
        Assert.assertEquals(2, blacklistSqls.size());
    }

    @Test
    public void testLoadSqls() throws IOException {
        // import multiple files
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        // add jdbc type sql
        MockMultipartFile file3 = new MockMultipartFile("sqls3.txt", "sqls3.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls3.txt")));

        MockMultipartFile exceptionFile = new MockMultipartFile("exception_file.sql", "exception_file.sql",
                "text/plain", "".getBytes());

        Mockito.when(favoriteRuleService.transformFileToSqls(exceptionFile, PROJECT)).thenThrow(IOException.class);

        Map<String, Object> result = favoriteRuleService
                .importSqls(new MultipartFile[] { file1, file2, file3, exceptionFile }, PROJECT);
        List<ImportSqlResponse> responses = (List<ImportSqlResponse>) result.get("data");
        Assert.assertEquals(10, responses.size());
        Assert.assertFalse(responses.get(0).isCapable());
        Assert.assertTrue(responses.get(8).isCapable());
        Assert.assertEquals(10, result.get("size"));
        Assert.assertEquals(3, result.get("capable_sql_num"));
        String failedFilesMsg = (String) result.get("msg");
        Assert.assertNotNull(failedFilesMsg);
        Assert.assertEquals("exception_file.sql parse failed", failedFilesMsg);

        // import empty file
        MockMultipartFile emptyFile = new MockMultipartFile("empty_file.sql", "empty_file.sql", "text/plain",
                "".getBytes());
        result = favoriteRuleService.importSqls(new MultipartFile[] { emptyFile }, PROJECT);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.get("size"));
    }

    @Test
    public void testGetRulesWithError() {
        // assert get rule error
        try {
            favoriteRuleService.getFavoriteRules(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(
                    String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), FavoriteRule.COUNT_RULE_NAME),
                    ex.getMessage());
        }
    }

    @Test
    public void testGetFilterRulesAndUpdate() {
        NFavoriteScheduler favoriteScheduler = Mockito.mock(NFavoriteScheduler.class);
        Mockito.doReturn(true).when(favoriteScheduler).hasStarted();
        Mockito.doReturn(favoriteScheduler).when(favoriteRuleService).getFavoriteScheduler(PROJECT);

        Map<String, Object> favoriteRuleResponse = favoriteRuleService.getFavoriteRules(PROJECT);

        Assert.assertTrue((boolean) favoriteRuleResponse.get("count_enable"));
        Assert.assertEquals(10.0f, favoriteRuleResponse.get("count_value"));

        List<String> users = (List<String>) favoriteRuleResponse.get("users");
        List<String> userGroups = (List<String>) favoriteRuleResponse.get("user_groups");
        Assert.assertTrue((boolean) favoriteRuleResponse.get("submitter_enable"));
        Assert.assertEquals(3, users.size());
        Assert.assertEquals(1, userGroups.size());

        long minDuration = (Long) favoriteRuleResponse.get("min_duration");
        long maxDuration = (Long) favoriteRuleResponse.get("max_duration");
        Assert.assertTrue((boolean) favoriteRuleResponse.get("duration_enable"));
        Assert.assertEquals(5, minDuration);
        Assert.assertEquals(8, maxDuration);

        // the request of updating frequency rule
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setFreqEnable(false);
        request.setFreqValue("0.2");
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(false);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));

        favoriteRuleService.updateRegularRule(PROJECT, request);
        Mockito.verify(favoriteScheduler).scheduleImmediately();

        // assert
        favoriteRuleResponse = favoriteRuleService.getFavoriteRules(PROJECT);
        Assert.assertFalse((boolean) favoriteRuleResponse.get("freq_enable"));
        Assert.assertFalse((boolean) favoriteRuleResponse.get("duration_enable"));
        Assert.assertFalse((boolean) favoriteRuleResponse.get("submitter_enable"));
        Assert.assertEquals(0.2, (float) favoriteRuleResponse.get("freq_value"), 0.1);

        users = (List<String>) favoriteRuleResponse.get("users");
        Assert.assertEquals(4, users.size());
        userGroups = (List<String>) favoriteRuleResponse.get("user_groups");
        Assert.assertEquals(0, userGroups.size());

        minDuration = (Long) favoriteRuleResponse.get("min_duration");
        maxDuration = (Long) favoriteRuleResponse.get("max_duration");
        Assert.assertEquals(0, minDuration);
        Assert.assertEquals(10, maxDuration);

        request.setUserGroups(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"));

        favoriteRuleService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = favoriteRuleService.getFavoriteRules(PROJECT);
        users = (List<String>) favoriteRuleResponse.get("users");
        Assert.assertEquals(4, users.size());
        userGroups = (List<String>) favoriteRuleResponse.get("user_groups");
        Assert.assertEquals(2, userGroups.size());
        Assert.assertEquals("ROLE_ADMIN", userGroups.get(0));

        // assert if favorite rules' values are empty
        request.setFreqEnable(false);
        request.setFreqValue(null);
        request.setDurationEnable(false);
        request.setMinDuration(null);
        request.setMaxDuration(null);

        favoriteRuleService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = favoriteRuleService.getFavoriteRules(PROJECT);
        Assert.assertNull(favoriteRuleResponse.get("freq_value"));
        Assert.assertNull(favoriteRuleResponse.get("min_duration"));
        Assert.assertNull(favoriteRuleResponse.get("max_duration"));
    }

    @Test
    public void testGetAccelerateRatio() {
        double ratio = favoriteRuleService.getAccelerateRatio(PROJECT);
        Assert.assertEquals(0, ratio, 0.1);
        AccelerateRatioManager ratioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        ratioManager.increment(100, 1000);
        ratio = favoriteRuleService.getAccelerateRatio(PROJECT);
        Assert.assertEquals(0.1, ratio, 0.1);
    }

    @Test
    public void testSqlValidate() {
        String sql = "select * from test_kylin_fact\n\n";
        var response = favoriteRuleService.sqlValidate(PROJECT, sql);
        Assert.assertTrue(response.isCapable());
    }

    @Test
    public void testSqlValidateError() {
        String sql = "select * from test_kylin\n\n";
        var response = favoriteRuleService.sqlValidate(PROJECT, sql);
        Assert.assertFalse(response.isCapable());
        Assert.assertEquals("Table 'TEST_KYLIN' not found.",
                Lists.newArrayList(response.getSqlAdvices()).get(0).getIncapableReason());
    }

    @Test
    public void testImportCCSQLs() {
        val ccDesc = new ComputedColumnDesc();
        ccDesc.setTableAlias("TEST_KYLIN_FACT");
        ccDesc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc.setColumnName("DEAL_AMOUNT");
        ccDesc.setDatatype("decimal(30,4)");
        ccDesc.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");

        val basicModel = NDataModelManager.getInstance(getTestConfig(), PROJECT)
                .getDataModelDescByAlias("nmodel_basic");
        Assert.assertTrue(basicModel.getComputedColumnDescs().contains(ccDesc));

        // PRICE * ITEM_COUNT expression already exists
        String sql = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";

        MockMultipartFile file1 = new MockMultipartFile("file.sql", "file.sql", "text/plain", sql.getBytes());
        Map<String, Object> result = favoriteRuleService.importSqls(new MultipartFile[] { file1 }, PROJECT);
        List<ImportSqlResponse> responses = (List<ImportSqlResponse>) result.get("data");
        Assert.assertEquals(1, responses.size());
        Assert.assertTrue(responses.get(0).isCapable());
        Assert.assertEquals(1, result.get("size"));
        Assert.assertEquals(1, result.get("capable_sql_num"));

        // same cc expression not replaced with existed cc
        Assert.assertEquals(sql, responses.get(0).getSql());
    }

    @Test
    public void testImportSqlExceedsLimit() throws Exception {
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        MockMultipartFile file4 = new MockMultipartFile("sqls4.sql", "sqls4.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls4.sql")));

        try {
            favoriteRuleService.importSqls(new MultipartFile[] { file4 }, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertEquals("Up to 1000 SQLs could be imported at a time", ex.getMessage());
        }

        try {
            favoriteRuleService.importSqls(new MultipartFile[] { file1, file2, file4 }, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertEquals("Up to 1000 SQLs could be imported at a time", ex.getMessage());
        }
    }
}
