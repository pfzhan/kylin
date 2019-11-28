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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.val;
import lombok.var;

public class NSmartSemiAutoTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testWithSource() throws IOException, NoSuchFieldException, IllegalAccessException {
        val project = "default";
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(project));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "true");

        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        modelManager.listAllModelIds().forEach(id -> {
            dataflowManager.dropDataflow(id);
            indexPlanManager.dropIndexPlan(id);
            modelManager.dropModel(id);
        });

        val baseModel = JsonUtil.readValue(new File("src/test/resources/nsmart/default/model_desc/model.json"),
                NDataModel.class);
        modelManager.createDataModelDesc(baseModel, "ADMIN");
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(baseModel.getUuid());
        indexPlan.setProject(project);
        indexPlanManager.createIndexPlan(indexPlan);
        dataflowManager.createDataflow(indexPlan, "ADMIN");

        val sql1 = "select sum(TEST_KYLIN_FACT.ITEM_COUNT) from TEST_KYLIN_FACT group by TEST_KYLIN_FACT.CAL_DT;";
        val sql2 = "select LEAF_CATEG_ID from TEST_KYLIN_FACT;";

        val spyFqManager = Mockito.spy(FavoriteQueryManager.getInstance(getTestConfig(), project));
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(FavoriteQueryManager.class).put(project, spyFqManager);

        Mockito.doAnswer(invocation -> {
            FavoriteQuery fq = new FavoriteQuery();
            fq.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
            return fq;
        }).when(spyFqManager).get(sql1);

        Mockito.doAnswer(invocation -> {
            FavoriteQuery fq = new FavoriteQuery();
            fq.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
            return fq;
        }).when(spyFqManager).get(sql2);

        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), project, new String[] { sql1 });
        smartMaster1.runOptRecommendation(null);

        val recommendationManager = OptimizeRecommendationManager.getInstance(getTestConfig(), project);
        var recommendation = recommendationManager.getOptimizeRecommendation(baseModel.getId());
        Assert.assertEquals(1, recommendation.getLayoutRecommendations().size());
        Assert.assertEquals(LayoutRecommendationItem.IMPORTED,
                recommendation.getLayoutRecommendations().get(0).getSource());

        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), project, new String[] { sql2 });
        smartMaster2.runOptRecommendation(null);
        recommendation = recommendationManager.getOptimizeRecommendation(baseModel.getId());
        Assert.assertEquals(2, recommendation.getLayoutRecommendations().size());
        Assert.assertEquals(LayoutRecommendationItem.QUERY_HISTORY,
                recommendation.getLayoutRecommendations().get(1).getSource());

    }
}
