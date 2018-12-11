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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.response.FavoriteRuleResponse;
import io.kyligence.kap.rest.response.UpdateWhitelistResponse;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FavoriteRuleServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    @Mock
    private FavoriteQueryService favoriteQueryService = Mockito.mock(FavoriteQueryService.class);

    @InjectMocks
    private FavoriteRuleService favoriteRuleService = Mockito.spy(new FavoriteRuleService());

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        ReflectionTestUtils.setField(favoriteRuleService, "favoriteQueryService", favoriteQueryService);
    }

    @Test
    public void testBlacklistCRUD() throws IOException {
        List<FavoriteRuleResponse> sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(1, sqls.size());
        // case of sql grammar is correct
        String validateSql = "select count(*) from test_kylin_fact";
        SQLValidateResult result = favoriteRuleService.appendSqlToBlacklist(validateSql, PROJECT);
        Assert.assertTrue(result.isCapable());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(2, sqls.size());

        // case of sql grammar is incorrect
        String inValidateSql = "select count(*) from not_exist_table";
        result = favoriteRuleService.appendSqlToBlacklist(inValidateSql, PROJECT);
        Assert.assertFalse(result.isCapable());
        Assert.assertEquals("Table 'NOT_EXIST_TABLE' not found.", result.getSqlAdvices().iterator().next().getIncapableReason());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(2, sqls.size());

        // case of having the same sql in whitelist
        String sameSqlInWhitelist = "select count(*) from test_account";
        try {
            favoriteRuleService.appendSqlToBlacklist(sameSqlInWhitelist, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getSQL_ALREADY_IN_WHITELIST(), ex.getMessage());
        }

        // case of already having a same sql in blacklist
        String sameSqlInBlacklist = "select * from test_country";
        result = favoriteRuleService.appendSqlToBlacklist(sameSqlInBlacklist, PROJECT);
        Assert.assertTrue(result.isCapable());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(2, sqls.size());

        // remove sql from blacklist
        FavoriteRule blacklist = favoriteRuleService.getFavoriteRuleManager(PROJECT).getByName(FavoriteRule.BLACKLIST_NAME);
        List<FavoriteRule.AbstractCondition> conditions = blacklist.getConds();
        FavoriteRule.SQLCondition firstCond = (FavoriteRule.SQLCondition) conditions.get(0);
        favoriteRuleService.removeBlacklistSql(firstCond.getId(), PROJECT);
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(1, sqls.size());
        Assert.assertNotEquals(firstCond.getId(), sqls.get(0).getId());
        Assert.assertNotEquals(firstCond.getSql(), sqls.get(0).getSql());
    }

    @Test
    public void testUpdateWhitelist() throws IOException {
        // case of updated sql is valid
        List<FavoriteRuleResponse> sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(2, sqls.size());
        String updatedSql = "select count(*) from test_country";
        favoriteRuleService.updateWhitelistSql(updatedSql, sqls.get(0).getId(), PROJECT);
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(updatedSql, sqls.get(0).getSql());
        // triggered accelerate
        Mockito.verify(favoriteQueryService).favoriteForWhitelistChannel(Mockito.anySet(), Mockito.anyString());

        // case of updated sql is invalid
        String invalidSql = "select * from not_exist_table";
        UpdateWhitelistResponse response = favoriteRuleService.updateWhitelistSql(invalidSql, sqls.get(0).getId(), PROJECT);
        Assert.assertFalse(response.isCapable());
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(updatedSql, sqls.get(0).getSql());

        // case of having the same sql in blacklist
        String sameSqlInBlacklist = "select * from test_country";
        try {
            favoriteRuleService.updateWhitelistSql(sameSqlInBlacklist, sqls.get(0).getId(), PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST(), ex.getMessage());
        }

        // case of having the same sql in whitelist
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        String origSql = sqls.get(0).getSql();
        String sameSqlInWhitelist = "select count(*) from test_account";
        try {
            favoriteRuleService.updateWhitelistSql(sameSqlInWhitelist, sqls.get(0).getId(), PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getSQL_IN_WHITELIST_OR_ID_NOT_FOUND(), ex.getMessage());
        }
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(origSql, sqls.get(0).getSql());
    }

    @Test
    public void testRemoveWhitelist() throws IOException {
        // originally, there are two sqls in white list
        List<FavoriteRuleResponse> sqls = favoriteRuleService.getWhitelist(PROJECT);
        // remove first one
        favoriteRuleService.removeWhitelistSql(sqls.get(0).getId(), PROJECT);
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(1, sqls.size());
        // remove last one
        favoriteRuleService.removeWhitelistSql(sqls.get(0).getId(), PROJECT);
        // now the white list is empty
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(0, sqls.size());
    }

    @Test
    public void testAppendSqlToWhitelist() throws IOException {
        // case of having conflict with blacklist
        String conflictSql = "select * from test_country";
        int sqlPatternHash = 1965162027;
        try {
            favoriteRuleService.appendSqlToWhitelist(conflictSql, sqlPatternHash, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST(), ex.getMessage());
        }

        // case of having the same sql in whitelist
        String sameSqlInWhitelist = "select * from test_account";
        favoriteRuleService.appendSqlToWhitelist(sameSqlInWhitelist, sameSqlInWhitelist.hashCode(), PROJECT);
        FavoriteRule whitelist = favoriteRuleService.getFavoriteRule(PROJECT, FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(2, whitelist.getConds().size());

        // case of append a sql to whitelist
        String sql = "test_sql";
        favoriteRuleService.appendSqlToWhitelist(sql, sql.hashCode(), PROJECT);
        whitelist = favoriteRuleService.getFavoriteRule(PROJECT, FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(3, whitelist.getConds().size());
    }

    @Test
    public void testLoadSqlsToWhitelist() throws IOException {
        // case of wrong file type
        MockMultipartFile file = new MockMultipartFile("wrong_file_type.csv", "wrong_file_type.csv", "text/plain", "".getBytes());
        try {
            favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(InternalErrorException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getUPLOADED_FILE_TYPE_OR_SIZE_IS_NOT_DESIRED(), ex.getMessage());
        }

        // case of empty file
        file = new MockMultipartFile("empty_content.sql", "empty_content.sql", "text/plain", "".getBytes());
        try {
            favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(InternalErrorException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getNO_SQL_FOUND(), ex.getMessage());
        }

        // case of sqls length is 0
        file = new MockMultipartFile("empty_sqls.sql", "empty_sqls.sql", "text/plain", ";;".getBytes());
        try {
            favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(InternalErrorException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getNO_SQL_FOUND(), ex.getMessage());
        }

        // case of sql length if 0 after trim
        file = new MockMultipartFile("empty_sqls_after_trim.sql", "empty_sqls_after_trim.sql", "text/plain", ";\n;\n;\n".getBytes());
        favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        List<FavoriteRuleResponse> sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(2, sqls.size());

        // case of having conflct with sqls in blacklist
        file = new MockMultipartFile("sqls_conflict_with_blacklist.sql", "sqls_conflict_with_blacklist.sql", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls_conflict_with_blacklist.sql")));
        try {
            favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST(), ex.getMessage());
        }

        // case of already have the same sql in whitelist
        file = new MockMultipartFile("sqls_already_in_whitelist.sql", "sqls_already_in_whitelist.sql", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls_already_in_whitelist.sql")));
        favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(3, sqls.size());
        Mockito.verify(favoriteQueryService).favoriteForWhitelistChannel(Mockito.anySet(), Mockito.anyString());

        // case of having 8 total sqls, and 2 of them are valid
        file = new MockMultipartFile("sqls.sql", "sqls.sql", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls.sql")));
        favoriteRuleService.loadSqlsToWhitelist(file, PROJECT);
        sqls = favoriteRuleService.getWhitelist(PROJECT);
        Assert.assertEquals(11, sqls.size());
        int capableSqlSize = 0;
        for (FavoriteRuleResponse response : sqls) {
            if (response.isCapable())
                capableSqlSize ++;
        }
        Assert.assertEquals(3, capableSqlSize);
        Assert.assertEquals(11, sqls.size());

        // two capable sqls in sqls.sql file
        String capableSqlPattern1 = "SELECT \"CAL_DT\", \"LSTG_FORMAT_NAME\", SUM(\"PRICE\")\n" +
                "FROM \"TEST_KYLIN_FACT\"\n" +
                "WHERE \"CAL_DT\" = '2010-01-01'\n" +
                "GROUP BY \"CAL_DT\", \"LSTG_FORMAT_NAME\"\n" +
                "LIMIT 1";
        String capableSqlPattern2 = "SELECT \"CAL_DT\", SUM(\"PRICE\")\n" +
                "FROM \"TEST_KYLIN_FACT\"\n" +
                "WHERE \"CAL_DT\" = '2010-01-01'\n" +
                "GROUP BY \"CAL_DT\"\n" +
                "LIMIT 1";
        Set<String> capableSqlPatterns = new HashSet<>();
        capableSqlPatterns.add(capableSqlPattern1);
        capableSqlPatterns.add(capableSqlPattern2);

        // only insert capable sql pattern to DAO
        Mockito.verify(favoriteQueryService).favoriteForWhitelistChannel(capableSqlPatterns, PROJECT);
    }

    @Test
    public void testGetRulesWithError() {
        // assert get rule error
        try {
            favoriteRuleService.getFrequencyRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    FavoriteRule.FREQUENCY_RULE_NAME), ex.getMessage());
        }

        try {
            favoriteRuleService.getSubmitterRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    FavoriteRule.SUBMITTER_RULE_NAME), ex.getMessage());
        }

        try {
            favoriteRuleService.getDurationRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    FavoriteRule.DURATION_RULE_NAME), ex.getMessage());
        }
    }

    @Test
    public void testGetFilterRulesAndUpdate() throws IOException {
        NFavoriteScheduler favoriteScheduler = Mockito.mock(NFavoriteScheduler.class);
        Mockito.doReturn(true).when(favoriteScheduler).hasStarted();
        Mockito.doReturn(favoriteScheduler).when(favoriteRuleService).getFavoriteScheduler(PROJECT);
        Map<String, Object> frequencyRuleResult = favoriteRuleService.getFrequencyRule(PROJECT);
        Assert.assertTrue((boolean) frequencyRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(0.1, (float) frequencyRuleResult.get("freqValue"), 0.1);

        Map<String, Object> submitterRuleResult = favoriteRuleService.getSubmitterRule(PROJECT);
        List<String> users = (ArrayList<String>) submitterRuleResult.get("users");
        Assert.assertTrue((boolean) submitterRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(3, users.size());

        Map<String, Object> durationRuleResult = favoriteRuleService.getDurationRule(PROJECT);
        List<Long> durationValues = (ArrayList<Long>) durationRuleResult.get("durationValue");
        Assert.assertTrue((boolean) durationRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(5, (long) durationValues.get(0));
        Assert.assertEquals(8, (long) durationValues.get(1));

        // the request of updating frequency rule
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        request.setFreqValue("0.2");

        favoriteRuleService.updateRegularRule(PROJECT, request, FavoriteRule.FREQUENCY_RULE_NAME);
        frequencyRuleResult = favoriteRuleService.getFrequencyRule(PROJECT);
        Assert.assertFalse((boolean) frequencyRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(0.2, (float) frequencyRuleResult.get("freqValue"), 0.1);
        Mockito.verify(favoriteScheduler).scheduleAutoFavorite();

        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));

        // update submitter rule
        favoriteRuleService.updateRegularRule(PROJECT, request, FavoriteRule.SUBMITTER_RULE_NAME);
        submitterRuleResult = favoriteRuleService.getSubmitterRule(PROJECT);
        users = (ArrayList<String>) submitterRuleResult.get("users");
        Assert.assertFalse((boolean) submitterRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(4, users.size());

        request.setDurationValue(new String[]{"0", "10"});

        // update duration rule
        favoriteRuleService.updateRegularRule(PROJECT, request, FavoriteRule.DURATION_RULE_NAME);
        durationRuleResult = favoriteRuleService.getDurationRule(PROJECT);
        durationValues = (ArrayList<Long>) durationRuleResult.get("durationValue");
        Assert.assertFalse((boolean) durationRuleResult.get(FavoriteRule.ENABLE));
        Assert.assertEquals(0, (long) durationValues.get(0));
        Assert.assertEquals(10, (long) durationValues.get(1));
    }

    @Test
    public void testGetFavoriteRuleOverallImpact() {
        // mock the case when already have 3 rule-based favorite queries in database
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(3).when(favoriteQueryManager).getRuleBasedSize();
        Mockito.doReturn(favoriteQueryManager).when(favoriteRuleService).getFavoriteQueryManager(PROJECT);

        // case of query history sql pattern is 0
        NFavoriteScheduler favoriteScheduler = Mockito.mock(NFavoriteScheduler.class);
        NFavoriteScheduler.FrequencyStatus frequencyStatus = favoriteScheduler.new FrequencyStatus(System.currentTimeMillis());
        Mockito.doReturn(frequencyStatus).when(favoriteScheduler).getOverAllStatus();
        Mockito.doReturn(favoriteScheduler).when(favoriteRuleService).getFavoriteScheduler(PROJECT);

        double result = favoriteRuleService.getFavoriteRuleOverallImpact(PROJECT);
        Assert.assertEquals(0, result, 0.1);

        // case of query history sql pattern size is greater than 0, which is 5
        Map<String, Integer> sqlPatternInProj = Maps.newHashMap();
        sqlPatternInProj.put("test_sql1", 1);
        sqlPatternInProj.put("test_sql2", 1);
        sqlPatternInProj.put("test_sql3", 1);
        sqlPatternInProj.put("test_sql4", 1);
        sqlPatternInProj.put("test_sql5", 1);
        frequencyStatus.setSqlPatternFreqMap(sqlPatternInProj);

        Mockito.doReturn(frequencyStatus).when(favoriteScheduler).getOverAllStatus();

        result = favoriteRuleService.getFavoriteRuleOverallImpact(PROJECT);
        Assert.assertEquals(0.6, result, 0.1);
    }
}
