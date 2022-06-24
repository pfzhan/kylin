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

package org.apache.kylin.job.execution;

import org.apache.kylin.common.KylinConfig;
import org.junit.Before;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;

public class JobStatusChangedTest extends NLocalFileMetadataTestCase {
    String project = "default";
    KylinConfig config;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        config = KylinConfig.getInstanceFromEnv();
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        prjMgr.createProject(project, "", "", Maps.newLinkedHashMap());
    }

    //TODO need to be rewritten
    /*
    @Test
    public void test_KE24110_FailSamplingJobWithEpochChanged() throws Exception {
        EpochManager epcMgr = EpochManager.getInstance();
        epcMgr.tryUpdateEpoch(project, true);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.setProject("default");

        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        job.addTask(task1);

        BaseTestExecutable task2 = new FiveSecondErrorTestExecutable();
        task2.setProject("default");

        job.addTask(task2);
        execMgr.addJob(job);

        config.setProperty("kylin.env", "dev");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        // record old avail mem
        final double before = NDefaultScheduler.currentAvailableMem();

        // wait until to_failed step2 is running
        ConditionFactory conditionFactory = with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(60000, TimeUnit.MILLISECONDS);
        conditionFactory.until(() -> ExecutableState.RUNNING == job.getTasks().get(1).getStatus());

        // after to_failed step2 is running, change epoch
        Epoch epoch = epcMgr.getEpoch(project);
        epoch.setEpochId(epoch.getEpochId() + 1);
        EpochStore epochStore = EpochStore.getEpochStore(config);
        epochStore.update(epoch);
        // wait util job finished
        conditionFactory.until(() -> before == NDefaultScheduler.currentAvailableMem());

        // to_failed step2 can not update job status due to epoch changed
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
    }

     */
}
