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
package io.kyligence.kap.rest.config.initialize;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BootstrapCommand implements Runnable {

    @Override
    public void run() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            initProject(kylinConfig, project);
        }

        for (val scheduler : NDefaultScheduler.listAllSchedulers()) {
            val project = scheduler.getProject();
            if (projectManager.getProject(scheduler.getProject()) == null) {
                EventOrchestratorManager.getInstance(kylinConfig).shutdownByProject(project);
                NFavoriteScheduler.shutdownByProject(project);
                NDefaultScheduler.shutdownByProject(project);
            }
        }
    }

    void initProject(KylinConfig config, final ProjectInstance project) {
        if (NFavoriteScheduler.getInstance(project.getName()).hasStarted()) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            EventOrchestratorManager.getInstance(config).addProject(project.getName());
            NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project.getName());
            scheduler.init(new JobEngineConfig(config), new MockJobLock());
            if (!scheduler.hasStarted()) {
                throw new RuntimeException("Scheduler for " + project.getName() + " has not been started");
            }

            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project.getName());
            favoriteScheduler.init();

            if (!favoriteScheduler.hasStarted()) {
                throw new RuntimeException(
                        "Auto favorite scheduler for " + project.getName() + " has not been started");
            }

            return 0;
        }, project.getName(), 1, UnitOfWork.DEFAULT_EPOCH_ID);

        log.info("init project {} finished", project.getName());
    }
}