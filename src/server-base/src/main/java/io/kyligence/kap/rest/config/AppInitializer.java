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
package io.kyligence.kap.rest.config;

import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.cluster.NodeCandidate;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.persistence.transaction.EventSynchronization;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Slf4j
@Component
public class AppInitializer {

    @EventListener(ContextRefreshedEvent.class)
    public void init(ContextRefreshedEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val eventStore = EventStore.getInstance(kylinConfig);
        val replayer = EventSynchronization.getInstance(kylinConfig);
        eventStore.syncEvents(replayer::replay);
        val candidate = new NodeCandidate(kylinConfig.getNodeId());

        val leaderInitiator = LeaderInitiator.getInstance(kylinConfig);
        leaderInitiator.start(candidate);

        if (leaderInitiator.isLeader()) {
            val projectManager = NProjectManager.getInstance(kylinConfig);
            for (ProjectInstance project : projectManager.listAllProjects()) {
                initProject(kylinConfig, project.getName(), true);
            }
        }
        EventListenerRegistry.getInstance(kylinConfig).register(new GlobalEventListener(), UnitOfWork.GLOBAL_UNIT);

        eventStore.startConsumer(replayer::replay);
        event.getApplicationContext().publishEvent(new AppInitializedEvent(event.getApplicationContext()));
    }

    static void initProject(KylinConfig config, String project, boolean needTransaction) {
        EventListenerRegistry.getInstance(config).register(new FavoriteQueryUpdateListener(), project);
        val leaderInitiator = LeaderInitiator.getInstance(config);
        if (!leaderInitiator.isLeader()) {
            return;
        }
        if (needTransaction) {
            UnitOfWork.doInTransactionWithRetry(() -> {
                initProjectSchedulers(config, project);
                return 0;
            }, project);
        } else {
            initProjectSchedulers(config, project);
        }
    }

    static void initProjectSchedulers(KylinConfig config, String project) {
        EventOrchestratorManager.getInstance(config).addProject(project);
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(config), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("Scheduler for " + project + " has not been started");
        }

        NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
        favoriteScheduler.init();

        if (!favoriteScheduler.hasStarted()) {
            throw new RuntimeException("Auto favorite scheduler for " + project + " has not been started");
        }

    }

    static class GlobalEventListener implements EventListenerRegistry.ResourceEventListener {

        @Override
        public void onUpdate(KylinConfig config, RawResource rawResource) {
            val term = rawResource.getResPath().split("\\/");
            if (term.length != 3 || !term[2].equals("project.json")) {
                return;
            }
            val project = term[1];
            log.debug("try to init project {}", project);
            initProject(config, project, false);
        }

        @Override
        public void onDelete(KylinConfig config, String resPath) {
            /// just implement it
        }
    }

    static class FavoriteQueryUpdateListener implements EventListenerRegistry.ResourceEventListener {
        @Override
        public void onUpdate(KylinConfig config, RawResource rawResource) {
            val term = rawResource.getResPath().split("\\/");
            if (!isFavoriteQueryPath(term))
                return;

            // deserialize
            FavoriteQuery favoriteQuery = deserialize(rawResource);
            if (favoriteQuery == null)
                return;

            // update favorite query map
            val project = term[1];
            FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);
            Map<String, FavoriteQuery> favoriteQueryMap = favoriteQueryManager.getFavoriteQueryMap();
            favoriteQueryMap.put(favoriteQuery.getSqlPattern(), favoriteQuery);
        }

        @Override
        public void onDelete(KylinConfig config, String resPath) {
            val term = resPath.split("\\/");
            if (!isFavoriteQueryPath(term))
                return;

            val project = term[1];
            FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);
            favoriteQueryManager.clearFavoriteQueryMap();
        }

        private boolean isFavoriteQueryPath(String[] term) {
            if (term.length < 3 || !term[2].equals(ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT.substring(1))) {
                return false;
            }

            return true;
        }

        private FavoriteQuery deserialize(RawResource rawResource) {
            FavoriteQuery favoriteQuery = null;
            try (InputStream is = rawResource.getByteSource().openStream(); DataInputStream din = new DataInputStream(is)) {
                favoriteQuery = new JsonSerializer<>(FavoriteQuery.class).deserialize(din);
            } catch (IOException e) {
                log.warn("error when deserializing resource: {}", rawResource.getResPath());
            }

            return favoriteQuery;
        }
    }

    public static class AppInitializedEvent extends ApplicationContextEvent {
        public AppInitializedEvent(ApplicationContext source) {
            super(source);
        }
    }
}
