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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.storage.DataflowCleaner;
import io.kyligence.kap.rest.storage.FavoriteQueryCleaner;
import io.kyligence.kap.rest.storage.GarbageCleaner;
import io.kyligence.kap.rest.storage.ModelCleaner;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MetadataCleanupService {

    @Scheduled(cron = "${kylin.garbage.metadata-cleanup-cron:0 0 * * * *}")
    public void clean() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            log.info("Start garbage collection for project<{}>", project.getName());
            try {
                cleanupProject(project);
            } catch (Exception e) {
                log.warn("clean project<" + project.getName() + "> failed", e);
            }
            log.info("Garbage collection for project<{}> finished", project.getName());
        }
    }

    public void cleanupProject(ProjectInstance project) throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());
            val favoriteQueryCleaner = new FavoriteQueryCleaner(project);
            val dataflowCleaner = new DataflowCleaner();
            val brokenModelCleaner = new ModelCleaner(true);
            val cleaners = Lists.newArrayList(favoriteQueryCleaner, dataflowCleaner);
            for (NDataModel model : dfManager.listUnderliningDataModels()) {
                for (GarbageCleaner cleaner : cleaners) {
                    cleaner.collect(model);
                }
            }
            for (GarbageCleaner cleaner : cleaners) {
                cleaner.cleanup();
            }
            if (MaintainModelType.MANUAL_MAINTAIN.equals(project.getMaintainModelType())) {
                return 0;
            }
            // clean checkBrokenWithRelatedInfo model only when project's maintainModelType is AUTO_MAINTAIN
            for (NDataModel model : dfManager.listUnderliningDataModels(true)) {
                brokenModelCleaner.collect(model);
            }
            brokenModelCleaner.cleanup();
            return 0;
        }, project.getName());
    }

}
