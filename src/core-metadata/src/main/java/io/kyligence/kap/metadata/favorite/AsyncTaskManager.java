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

package io.kyligence.kap.metadata.favorite;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncTaskManager {

    public static final String ASYNC_ACCELERATION_TASK = "async_acceleration_task";

    public static final Serializer<AsyncAccelerationTask> ASYNC_ACCELERATION_SERIALIZER = new JsonSerializer<>(
            AsyncAccelerationTask.class);

    private final KylinConfig kylinConfig;
    private final ResourceStore resourceStore;
    private final String resourceRootPath;

    private AsyncTaskManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            log.info("Initializing AccelerateTagManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRootPath = "/" + project + ResourceStore.ASYNC_TASK;
    }

    // called by reflection
    static AsyncTaskManager newInstance(KylinConfig config, String project) {
        return new AsyncTaskManager(config, project);
    }

    public static AsyncTaskManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, AsyncTaskManager.class);
    }

    private String path(String uuid) {
        return this.resourceRootPath + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(AsyncAccelerationTask asyncTask) {
        if (asyncTask.getTaskType().equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            resourceStore.checkAndPutResource(path(asyncTask.getUuid()), asyncTask, ASYNC_ACCELERATION_SERIALIZER);
        }
    }

    public AbstractAsyncTask get(String taskType) {
        List<AsyncAccelerationTask> asyncAccelerationTaskList = Lists.newArrayList();
        if (taskType.equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            asyncAccelerationTaskList = resourceStore.getAllResources(resourceRootPath, ASYNC_ACCELERATION_SERIALIZER);
            if (asyncAccelerationTaskList.isEmpty()) {
                return new AsyncAccelerationTask(false, Maps.newHashMap(), ASYNC_ACCELERATION_TASK);
            }
        }
        return asyncAccelerationTaskList.get(0);
    }

    public static void resetAccelerationTagMap(String project) {
        log.info("reset acceleration tag for project({})", project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.setAlreadyRunning(false);
            asyncAcceleration.setUserRefreshedTagMap(Maps.newHashMap());
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
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
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.getUserRefreshedTagMap().put(userName, false);
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
        log.info("clean acceleration tag successfully for project({}: by user {})", project, userName);
    }
}
