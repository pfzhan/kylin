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

package org.apache.kylin.job.dao;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.val;

/**
 */
public class NExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);
    private static final Logger logger = LoggerFactory.getLogger(NExecutableDao.class);

    public static NExecutableDao getInstance(KylinConfig config, String project) {
        return config.getManager(project, NExecutableDao.class);
    }

    // called by reflection
    static NExecutableDao newInstance(KylinConfig config, String project) {
        return new NExecutableDao(config, project);
    }
    // ============================================================================

    private String project;

    private KylinConfig config;

    private CachedCrudAssist<ExecutablePO> crud;

    private NExecutableDao(KylinConfig config, String project) {
        logger.trace("Using metadata url: {}", config);
        this.project = project;
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<ExecutablePO>(getStore(), resourceRootPath, "", ExecutablePO.class) {
            @Override
            protected ExecutablePO initEntityAfterReload(ExecutablePO entity, String resourceName) {
                entity.setProject(project);
                return entity;
            }
        };
    }

    public List<ExecutablePO> getJobs() {
        return crud.listAll();
    }

    public List<ExecutablePO> getPartialJobs(Predicate<String> predicate) {
        return crud.listPartial(predicate);
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) {
        return crud.listAll().stream()
                .filter(x -> x.getLastModified() >= timeStart && x.getLastModified() < timeEndExclusive)
                .collect(Collectors.toList());
    }

    public List<String> getJobIds() {
        return crud.listAll().stream()
                .sorted(Comparator.comparing(ExecutablePO::getCreateTime))
                .sorted(Comparator.comparing(ExecutablePO::getPriority))
                .map(RootPersistentEntity::resourceName).collect(Collectors.toList());
    }

    public ExecutablePO getJobByUuid(String uuid) {
        return crud.get(uuid);
    }

    public ExecutablePO addJob(ExecutablePO job) {
        if (getJobByUuid(job.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
        }
        crud.save(job);
        return job;
    }

    // for ut
    @VisibleForTesting
    public void deleteAllJob(){
        getJobs().forEach(job -> deleteJob(job.getId()));
    }

    public void deleteJob(String uuid) {
        crud.delete(uuid);
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater) {
        val job = getJobByUuid(uuid);
        Preconditions.checkNotNull(job);
        val copyForWrite = JsonUtil.copyBySerialization(job, JOB_SERIALIZER, null);
        if (updater.test(copyForWrite)) {
            crud.save(copyForWrite);
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

}
