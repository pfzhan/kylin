/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.favorite;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.favorite.AsyncTaskStore;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class AsyncTaskManager {

    public static final String ASYNC_ACCELERATION_TASK = "async_acceleration_task";
    public static final List<String> ALL_TASK_TYPES = Collections.singletonList(ASYNC_ACCELERATION_TASK);

    private final AsyncTaskStore asyncTaskStore;
    private final String project;

    private AsyncTaskManager(String project) throws Exception {
        this.project = project;
        this.asyncTaskStore = new AsyncTaskStore(KylinConfig.getInstanceFromEnv());
    }

    public static AsyncTaskManager getInstance(String project) {
        return Singletons.getInstance(project, AsyncTaskManager.class);
    }

    public DataSourceTransactionManager getTransactionManager() {
        return asyncTaskStore.getTransactionManager();
    }

    public void save(AsyncAccelerationTask asyncTask) {
        if (asyncTask.getTaskType().equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            saveOrUpdate(asyncTask);
        }
    }

    private void saveOrUpdate(AbstractAsyncTask asyncTask) {
        if (asyncTask.getId() == 0) {
            asyncTask.setProject(project);
            asyncTask.setCreateTime(System.currentTimeMillis());
            asyncTask.setUpdateTime(asyncTask.getCreateTime());
            asyncTaskStore.save(asyncTask);
        } else {
            asyncTask.setUpdateTime(System.currentTimeMillis());
            asyncTaskStore.update(asyncTask);
        }
    }

    public AbstractAsyncTask get(String taskType) {
        if (!taskType.equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            throw new IllegalArgumentException("TaskType " + taskType + "is not supported!");
        }
        AbstractAsyncTask syncTask = asyncTaskStore.queryByType(project, ASYNC_ACCELERATION_TASK);
        if (syncTask == null) {
            return new AsyncAccelerationTask(false, Maps.newHashMap(), ASYNC_ACCELERATION_TASK);
        } else {
            return AsyncAccelerationTask.copyFromAbstractTask(syncTask);
        }
    }

    public static void resetAccelerationTagMap(String project) {
        log.info("reset acceleration tag for project({})", project);
        AsyncTaskManager manager = getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) manager.get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.setAlreadyRunning(false);
            asyncAcceleration.setUserRefreshedTagMap(Maps.newHashMap());
            manager.save(asyncAcceleration);
            return null;
        });
        log.info("rest acceleration tag successfully for project({})", project);
    }

    public static void cleanAccelerationTagByUser(String project, String userName) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        if (!projectInstance.isSemiAutoMode()) {
            log.debug("Recommendation is forbidden of project({}), there's no need to clean acceleration tag", project);
            return;
        }

        if (!EpochManager.getInstance().checkEpochOwner(project) && !kylinConfig.isUTEnv()) {
            return;
        }

        log.info("start to clean acceleration tag by user");
        AsyncTaskManager manager = getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) manager.get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.getUserRefreshedTagMap().put(userName, false);
            manager.save(asyncAcceleration);
            return null;
        });
        log.info("clean acceleration tag successfully for project({}: by user {})", project, userName);
    }
}
