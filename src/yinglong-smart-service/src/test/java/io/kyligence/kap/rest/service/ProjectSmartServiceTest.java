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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.kylin.rest.constant.Constant;
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
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.response.ProjectStatisticsResponse;
import io.kyligence.kap.rest.service.task.RecommendationTopNUpdateScheduler;

public class ProjectSmartServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(ProjectSmartService.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(RawRecService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setUp() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);
        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                new RecommendationTopNUpdateScheduler());
        ReflectionTestUtils.setField(projectSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectSmartService, "projectSmartSupporter", rawRecService);
        ReflectionTestUtils.setField(projectSmartService, "projectModelSupporter", modelService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetFavoriteRules() {
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(true, favoriteRuleResponse.get("count_enable"));
        assertEquals(10.0f, favoriteRuleResponse.get("count_value"));
        assertEquals(Lists.newArrayList("userA", "userB", "userC"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRuleResponse.get("user_groups"));
        assertEquals(5L, favoriteRuleResponse.get("min_duration"));
        assertEquals(8L, favoriteRuleResponse.get("max_duration"));
        assertEquals(true, favoriteRuleResponse.get("duration_enable"));
    }

    @Test
    public void testUpdateFavoriteRules() {
        RecommendationTopNUpdateScheduler recommendationTopNUpdateScheduler = new RecommendationTopNUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                recommendationTopNUpdateScheduler);
        // update with FavoriteRuleUpdateRequest and assert
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(false);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        request.setMinHitCount("11");
        request.setEffectiveDays("11");
        request.setUpdateFrequency("3");

        projectSmartService.updateRegularRule(PROJECT, request);
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(false, favoriteRuleResponse.get("duration_enable"));
        assertEquals(false, favoriteRuleResponse.get("submitter_enable"));
        assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList(), favoriteRuleResponse.get("user_groups"));
        assertEquals(0L, favoriteRuleResponse.get("min_duration"));
        assertEquals(10L, favoriteRuleResponse.get("max_duration"));
        assertEquals(true, favoriteRuleResponse.get("recommendation_enable"));
        assertEquals(30L, favoriteRuleResponse.get("recommendations_value"));
        assertEquals(false, favoriteRuleResponse.get("excluded_tables_enable"));
        assertEquals("", favoriteRuleResponse.get("excluded_tables"));
        assertEquals(11, favoriteRuleResponse.get("min_hit_count"));
        assertEquals(11, favoriteRuleResponse.get("effective_days"));
        assertEquals(3, favoriteRuleResponse.get("update_frequency"));

        // check excluded_tables
        request.setExcludeTablesEnable(true);
        request.setExcludedTables("a.a,b.b,c.c");
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(true, favoriteRuleResponse.get("excluded_tables_enable"));
        assertEquals("a.a,b.b,c.c", favoriteRuleResponse.get("excluded_tables"));
        // check excluded_tables
        request.setExcludeTablesEnable(false);
        request.setExcludedTables(null);
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(false, favoriteRuleResponse.get("excluded_tables_enable"));
        assertEquals("", favoriteRuleResponse.get("excluded_tables"));

        // check user_groups
        request.setUserGroups(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"));
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"), favoriteRuleResponse.get("user_groups"));

        // assert if favorite rules' values are empty
        request.setFreqEnable(false);
        request.setFreqValue(null);
        request.setDurationEnable(false);
        request.setMinDuration(null);
        request.setMaxDuration(null);
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertNull(favoriteRuleResponse.get("freq_value"));
        Assert.assertNull(favoriteRuleResponse.get("min_duration"));
        Assert.assertNull(favoriteRuleResponse.get("max_duration"));
        recommendationTopNUpdateScheduler.close();
    }

    @Test
    public void testResetFavoriteRules() {
        // reset
        projectService.resetProjectConfig(PROJECT, "favorite_rule_config");
        Map<String, Object> favoriteRules = projectSmartService.getFavoriteRules(PROJECT);

        assertEquals(false, favoriteRules.get("freq_enable"));
        assertEquals(0.1f, favoriteRules.get("freq_value"));

        assertEquals(true, favoriteRules.get("count_enable"));
        assertEquals(10.0f, favoriteRules.get("count_value"));

        assertEquals(true, favoriteRules.get("submitter_enable"));
        assertEquals(Lists.newArrayList("ADMIN"), favoriteRules.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRules.get("user_groups"));

        assertEquals(false, favoriteRules.get("duration_enable"));
        assertEquals(0L, favoriteRules.get("min_duration"));
        assertEquals(180L, favoriteRules.get("max_duration"));

        assertEquals(true, favoriteRules.get("recommendation_enable"));
        assertEquals(20L, favoriteRules.get("recommendations_value"));

        assertEquals(false, favoriteRules.get("excluded_tables_enable"));
        assertEquals("", favoriteRules.get("excluded_tables"));

        assertEquals(30, favoriteRules.get("min_hit_count"));
        assertEquals(2, favoriteRules.get("effective_days"));
        assertEquals(2, favoriteRules.get("update_frequency"));

    }

    @Test
    public void testGetProjectStatistics() {
        RecommendationTopNUpdateScheduler recommendationTopNUpdateScheduler = new RecommendationTopNUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                recommendationTopNUpdateScheduler);
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("gc_test");
        assertEquals(1, projectStatistics.getDatabaseSize());
        assertEquals(1, projectStatistics.getTableSize());
        assertEquals(0, projectStatistics.getLastWeekQueryCount());
        assertEquals(0, projectStatistics.getUnhandledQueryCount());
        assertEquals(0, projectStatistics.getAdditionalRecPatternCount());
        assertEquals(0, projectStatistics.getRemovalRecPatternCount());
        assertEquals(0, projectStatistics.getRecPatternCount());
        assertEquals(7, projectStatistics.getEffectiveRuleSize());
        assertEquals(0, projectStatistics.getApprovedRecCount());
        assertEquals(0, projectStatistics.getApprovedAdditionalRecCount());
        assertEquals(0, projectStatistics.getApprovedRemovalRecCount());
        assertEquals(2, projectStatistics.getModelSize());
        assertEquals(0, projectStatistics.getAcceptableRecSize());
        Assert.assertFalse(projectStatistics.isRefreshed());
        assertEquals(20, projectStatistics.getMaxRecShowSize());

        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject("gc_test");
        request.setExcludeTablesEnable(true);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(true);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        request.setUpdateFrequency("1");
        projectSmartService.updateRegularRule("gc_test", request);
        ProjectStatisticsResponse projectStatistics2 = projectSmartService.getProjectStatistics("gc_test");
        assertEquals(7, projectStatistics2.getEffectiveRuleSize());

        ProjectStatisticsResponse statisticsOfProjectDefault = projectSmartService.getProjectStatistics(PROJECT);
        assertEquals(3, statisticsOfProjectDefault.getDatabaseSize());
        assertEquals(20, statisticsOfProjectDefault.getTableSize());
        assertEquals(0, statisticsOfProjectDefault.getLastWeekQueryCount());
        assertEquals(0, statisticsOfProjectDefault.getUnhandledQueryCount());
        assertEquals(-1, statisticsOfProjectDefault.getAdditionalRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getRemovalRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getEffectiveRuleSize());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedRecCount());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedAdditionalRecCount());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedRemovalRecCount());
        assertEquals(8, statisticsOfProjectDefault.getModelSize());
        assertEquals(-1, statisticsOfProjectDefault.getAcceptableRecSize());
        Assert.assertFalse(statisticsOfProjectDefault.isRefreshed());
        assertEquals(-1, statisticsOfProjectDefault.getMaxRecShowSize());

        ProjectStatisticsResponse statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(2, statsOfPrjStreamingTest.getDatabaseSize());
        assertEquals(11, statsOfPrjStreamingTest.getTableSize());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(1, statsOfPrjStreamingTest.getDatabaseSize());
        assertEquals(6, statsOfPrjStreamingTest.getTableSize());

        recommendationTopNUpdateScheduler.close();
    }

    @Test
    public void testGetStreamingProjectStatistics() {
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(2, projectStatistics.getDatabaseSize());
        assertEquals(11, projectStatistics.getTableSize());
        assertEquals(0, projectStatistics.getLastWeekQueryCount());
        assertEquals(0, projectStatistics.getUnhandledQueryCount());
        assertEquals(11, projectStatistics.getModelSize());
    }

}
