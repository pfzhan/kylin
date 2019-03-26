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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BootstrapCommand implements Runnable {

    @Override
    public void run() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            initProject(kylinConfig, project.getName());
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

    static void initProject(KylinConfig config, String project) {
        val leaderInitiator = LeaderInitiator.getInstance(config);
        if (!leaderInitiator.isLeader()) {
            return;
        }
        if (NFavoriteScheduler.getInstance(project).hasStarted()) {
            return;
        }
        UnitOfWork.doInTransactionWithRetry(() -> {
            EventOrchestratorManager.getInstance(config).addProject(project);
            NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
            scheduler.init(new JobEngineConfig(config), new MockJobLock());
            if (!scheduler.hasStarted()) {
                throw new RuntimeException("Scheduler for " + project + " has not been started");
            }

            createDefaultRules(project);

            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
            favoriteScheduler.init();

            if (!favoriteScheduler.hasStarted()) {
                throw new RuntimeException("Auto favorite scheduler for " + project + " has not been started");
            }
            return 0;
        }, project, 1);
        log.info("init project {} finished", project);
    }

    static void createDefaultRules(String projectName) {
        // create default rules
        // frequency rule
        val favoriteRuleManager = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        FavoriteRule.Condition freqCond = new FavoriteRule.Condition();
        freqCond.setRightThreshold("0.1");
        FavoriteRule freqRule = new FavoriteRule(Lists.newArrayList(freqCond), FavoriteRule.FREQUENCY_RULE_NAME, true);
        favoriteRuleManager.createRule(freqRule);
        // submitter rule
        FavoriteRule.Condition submitterCond = new FavoriteRule.Condition();
        submitterCond.setRightThreshold("ADMIN");
        FavoriteRule submitterRule = new FavoriteRule(Lists.newArrayList(submitterCond),
                FavoriteRule.SUBMITTER_RULE_NAME, true);
        favoriteRuleManager.createRule(submitterRule);
        // submitter group rule
        FavoriteRule.Condition submitterGroupCond = new FavoriteRule.Condition();
        submitterGroupCond.setRightThreshold("ROLE_ADMIN");
        favoriteRuleManager.createRule(new FavoriteRule(Lists.newArrayList(submitterGroupCond), FavoriteRule.SUBMITTER_GROUP_RULE_NAME, true));
        // duration rule
        FavoriteRule.Condition durationCond = new FavoriteRule.Condition();
        durationCond.setLeftThreshold("0");
        durationCond.setRightThreshold("180");
        FavoriteRule durationRule = new FavoriteRule(Lists.newArrayList(durationCond), FavoriteRule.DURATION_RULE_NAME,
                false);
        favoriteRuleManager.createRule(durationRule);

        // create blacklist
        FavoriteRule blacklist = new FavoriteRule();
        blacklist.setName(FavoriteRule.BLACKLIST_NAME);
        favoriteRuleManager.createRule(blacklist);
    }
}