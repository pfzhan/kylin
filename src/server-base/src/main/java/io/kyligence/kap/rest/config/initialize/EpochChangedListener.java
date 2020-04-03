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

import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.rest.util.CreateAdminUserUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;

import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class EpochChangedListener implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(EpochChangedListener.class);

    private static final String GLOBAL = "_global";

    @Autowired
    Environment env;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Subscribe
    public void onProjectControlled(ProjectControlledNotifier notifier) throws IOException {
        String project = notifier.getProject();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        logger.info("start thread of project: {}", project);
        if (!GLOBAL.equals(project)) {
            if (NFavoriteScheduler.getInstance(project).hasStarted()) {
                return;
            }
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
                scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
                if (!scheduler.hasStarted()) {
                    throw new RuntimeException("Scheduler for " + project + " has not been started");
                }

                NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
                favoriteScheduler.init();

                if (!favoriteScheduler.hasStarted()) {
                    throw new RuntimeException(
                            "Auto favorite scheduler for " + project + " has not been started");
                }
                return 0;
            }, project, 1);
            logger.info("Register project metrics for {}", project);
            NMetricsRegistry.registerProjectMetrics(kylinConfig, project);
        } else {
            CreateAdminUserUtils.createAllAdmins(userService, env);
            logger.info("Register global metrics...");
            NMetricsRegistry.registerGlobalMetrics(kylinConfig);
        }
        EventOrchestratorManager.getInstance(kylinConfig).addProject(project);
    }

    @Subscribe
    public void onProjectEscaped(ProjectEscapedNotifier notifier) {
        String project = notifier.getProject();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!GLOBAL.equals(project)) {
            logger.info("Shutdown related thread: {}", project);
            try {
                NExecutableManager.getInstance(kylinConfig, project).destoryAllProcess();
                NFavoriteScheduler.shutdownByProject(project);
                NDefaultScheduler.shutdownByProject(project);
            } catch (Exception e) {
                logger.warn("error when shutdown " + project + " thread", e);
            }
            logger.info("Remove project metrics for {}", project);
            NMetricsGroup.removeProjectMetrics(project);
        } else {
            logger.info("Remove global metrics...");
            NMetricsGroup.removeGlobalMetrics();
        }
        EventOrchestratorManager.getInstance(kylinConfig).shutdownByProject(project);
    }

    @Subscribe
    public void onEpochStarted(EpochStartedNotifier notifier) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.setChecker((event) -> {
            String instance = event.getInstance();
            String localIdentify = EpochOrchestrator.getOwnerIdentity().split("\\|")[0];
            return localIdentify.equalsIgnoreCase(instance);
        });
        resourceStore.leaderCatchup();
    }
}
