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

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.rest.service.task.QueryHistoryAccelerateScheduler;
import io.kyligence.kap.rest.util.CreateAdminUserUtils;
import io.kyligence.kap.rest.util.InitUserGroupUtils;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class EpochChangedListener implements IKeep {

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
        if (!GLOBAL.equals(project)) {
            if (NDefaultScheduler.getInstance(project).hasStarted()) {
                return;
            }
            log.info("start thread of project: {}", project);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
                scheduler.init(new JobEngineConfig(kylinConfig));
                if (!scheduler.hasStarted()) {
                    throw new RuntimeException("Scheduler for " + project + " has not been started");
                }

                QueryHistoryAccelerateScheduler qhAccelerateScheduler = QueryHistoryAccelerateScheduler
                        .getInstance(project);
                qhAccelerateScheduler.init();

                if (!qhAccelerateScheduler.hasStarted()) {
                    throw new RuntimeException(
                            "Query history accelerate scheduler for " + project + " has not been started");
                }
                return 0;
            }, project, 1);
        } else {
            //TODO need global leader
            CreateAdminUserUtils.createAllAdmins(userService, env);
            InitUserGroupUtils.initUserGroups(env);
            SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv()).updateSourceUsage();
            UnitOfWork.doInTransactionWithRetry(() -> {
                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
                return null;
            }, "", 1);
        }
    }

    @Subscribe
    public void onProjectEscaped(ProjectEscapedNotifier notifier) {
        String project = notifier.getProject();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!GLOBAL.equals(project)) {
            log.info("Shutdown related thread: {}", project);
            try {
                NExecutableManager.getInstance(kylinConfig, project).destoryAllProcess();
                QueryHistoryAccelerateScheduler.shutdownByProject(project);
                NDefaultScheduler.shutdownByProject(project);
            } catch (Exception e) {
                log.warn("error when shutdown " + project + " thread", e);
            }
        }
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
