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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.val;

public class CSVSourceTestCase extends ServiceTestBase {

    Map<Object, Object> originManager;

    protected String getProject() {
        return "default";
    }

    @Before
    public void setup() {
        super.setup();
        originManager = Maps.newHashMap();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(getProject());
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    protected void setupPushdownEnv() throws Exception {
        Class.forName("org.h2.Driver");
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        getTestConfig().setProperty("kylin.query.pushdown.partition-check.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "true");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    protected void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }

    public EpochManager spyEpochManager() throws NoSuchFieldException, IllegalAccessException {
        return spyManager(EpochManager.getInstance(getTestConfig()), EpochManager.class);
    }

    public OptimizeRecommendationManager spyOptimizeRecommendationManager()
            throws NoSuchFieldException, IllegalAccessException {
        return spyManagerByProject(OptimizeRecommendationManager.getInstance(getTestConfig(), getProject()),
                OptimizeRecommendationManager.class);
    }

    public NDataModelManager spyNDataModelManager() throws NoSuchFieldException, IllegalAccessException {
        return spyManagerByProject(NDataModelManager.getInstance(getTestConfig(), getProject()),
                NDataModelManager.class);
    }

    public NIndexPlanManager spyNIndexPlanManager() throws NoSuchFieldException, IllegalAccessException {
        return spyManagerByProject(NIndexPlanManager.getInstance(getTestConfig(), getProject()),
                NIndexPlanManager.class);
    }

    public NDataflowManager spyNDataflowManager() throws NoSuchFieldException, IllegalAccessException {
        return spyManagerByProject(NDataflowManager.getInstance(getTestConfig(), getProject()), NDataflowManager.class);
    }

    <T> T spyManagerByProject(T t, Class<T> tClass) throws NoSuchFieldException, IllegalAccessException {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(tClass).put(getProject(), manager);
        return manager;
    }

    <T> T spyManager(T t, Class<T> tClass) throws NoSuchFieldException, IllegalAccessException {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        Field filed = getTestConfig().getClass().getDeclaredField("managersCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, Object> managersCache = (ConcurrentHashMap<Class, Object>) filed.get(getTestConfig());
        managersCache.put(tClass, manager);
        return manager;
    }

    <T, M> T spy(M m, Function<M, T> functionM, Function<T, T> functionT) {
        return functionM.apply(Mockito.doAnswer(answer -> {
            T t = functionM.apply((M) originManager.get(m));
            return functionT.apply(t);
        }).when(m));
    }
}
