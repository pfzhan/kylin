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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Comparator;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.After;
import org.junit.Before;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class CSVSourceTestCase extends ServiceTestBase {

    protected String getProject() {
        return "default";
    }

    @Before
    public void setup() {
        super.setup();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(getProject());
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
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

        overwriteSystemProp("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        overwriteSystemProp("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overwriteSystemProp("kylin.query.pushdown.jdbc.username", "sa");
        overwriteSystemProp("kylin.query.pushdown.jdbc.password", "");
    }

    protected void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
    }

    public EpochManager spyEpochManager() throws Exception {
        return spyManager(EpochManager.getInstance(), EpochManager.class);
    }

    public NDataModelManager spyNDataModelManager() throws Exception {
        return spyManagerByProject(NDataModelManager.getInstance(getTestConfig(), getProject()),
                NDataModelManager.class, getProject());
    }

    public NIndexPlanManager spyNIndexPlanManager() throws Exception {
        return spyManagerByProject(NIndexPlanManager.getInstance(getTestConfig(), getProject()),
                NIndexPlanManager.class, getProject());
    }

    public NDataflowManager spyNDataflowManager() throws Exception {
        return spyManagerByProject(NDataflowManager.getInstance(getTestConfig(), getProject()), NDataflowManager.class,
                getProject());
    }

    protected List<AbstractExecutable> getRunningExecutables(String project, String model) {
        List<AbstractExecutable> runningExecutables = NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getRunningExecutables(project, model);
        runningExecutables.sort(Comparator.comparing(AbstractExecutable::getCreateTime));
        return runningExecutables;
    }

    protected void deleteJobByForce(AbstractExecutable executable) {
        val exManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        exManager.updateJobOutput(executable.getId(), ExecutableState.DISCARDED);
        exManager.deleteJob(executable.getId());
    }

}
