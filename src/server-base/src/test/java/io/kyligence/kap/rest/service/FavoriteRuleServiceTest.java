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
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.rest.response.ImportSqlResponse;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class FavoriteRuleServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    @InjectMocks
    private FavoriteRuleService favoriteRuleService = Mockito.spy(new FavoriteRuleService());

    @Before
    public void setUp() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @Test
    public void testBlacklistBasics() {
        final String sqlPattern1 = "test sql pattern 1";
        final String sqlPattern2 = "test sql pattern 2";

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery(sqlPattern1);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        FavoriteQuery favoriteQuery2 = new FavoriteQuery(sqlPattern2);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
        favoriteQueryManager.create(new HashSet(){{add(favoriteQuery1);add(favoriteQuery2);}});
        List<FavoriteQuery> favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(2, favoriteQueries.size());
        List<FavoriteRule.SQLCondition> sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(1, sqls.size());

        // append sql pattern1 to blacklist from rule-based channel and delete FQ
        favoriteRuleService.deleteFavoriteQuery(PROJECT, favoriteQueryManager.get(sqlPattern1).getUuid());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(2, sqls.size());
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(1, favoriteQueries.size());

        // append sql pattern2 to blacklist from imported channel and delete FQ
        favoriteRuleService.deleteFavoriteQuery(PROJECT, favoriteQueryManager.get(sqlPattern2).getUuid());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(2, sqls.size());
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(0, favoriteQueries.size());

        // append same sql to blacklist
        FavoriteQuery sqlPattern1FQ = new FavoriteQuery(sqlPattern1);
        sqlPattern1FQ.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        favoriteQueryManager.create(new HashSet(){{add(sqlPattern1FQ);}});
        favoriteRuleService.deleteFavoriteQuery(PROJECT, favoriteQueryManager.get(sqlPattern1).getUuid());
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(0, favoriteQueries.size());

        // returned blacklist sql is sorted by create time
        FavoriteRule.SQLCondition sqlCondition1 = sqls.get(0);
        FavoriteRule.SQLCondition sqlCondition2 = sqls.get(1);
        Assert.assertTrue(sqlCondition1.getCreateTime() > sqlCondition2.getCreateTime());

        // remove sql pattern from blacklist
        favoriteRuleService.removeBlacklistSql(sqls.get(0).getId(), PROJECT);
        sqls = favoriteRuleService.getBlacklistSqls(PROJECT);
        Assert.assertEquals(1, sqls.size());
    }

    @Test
    public void testLoadSqls() throws IOException {
        // import multiple files
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        MockMultipartFile exceptionFile = new MockMultipartFile("exception_file.sql", "exception_file.sql", "text/plain", "".getBytes());

        Mockito.when(favoriteRuleService.transformFileToSqls(exceptionFile)).thenThrow(IOException.class);

        Map<String, Object> result = favoriteRuleService.importSqls(new MultipartFile[]{file1, file2, exceptionFile}, PROJECT);
        List<ImportSqlResponse> responses = (List<ImportSqlResponse>) result.get("data");
        Assert.assertEquals(9, responses.size());
        Assert.assertFalse(responses.get(0).isCapable());
        Assert.assertTrue(responses.get(8).isCapable());
        Assert.assertEquals(9, result.get("size"));
        Assert.assertEquals(3, result.get("capable_sql_num"));
        String failedFilesMsg = (String) result.get("msg");
        Assert.assertNotNull(failedFilesMsg);
        Assert.assertEquals("exception_file.sql parse failed", failedFilesMsg);

        // import empty file
        MockMultipartFile emptyFile = new MockMultipartFile("empty_file.sql", "empty_file.sql", "text/plain", "".getBytes());
        result = favoriteRuleService.importSqls(new MultipartFile[]{emptyFile}, PROJECT);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.get("size"));
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
    public void testGetAccelerateRatio() {
        double ratio = favoriteRuleService.getAccelerateRatio(PROJECT);
        Assert.assertEquals(0, ratio, 0.1);
        AccelerateRatioManager ratioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        ratioManager.increment(100, 1000);
        ratio = favoriteRuleService.getAccelerateRatio(PROJECT);
        Assert.assertEquals(0.1, ratio, 0.1);
    }
}
