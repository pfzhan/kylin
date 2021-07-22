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

package io.kyligence.kap.streaming.manager;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobManager {

    private String project;

    private KylinConfig config;

    private CachedCrudAssist<StreamingJobMeta> crud;

    public static StreamingJobManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, StreamingJobManager.class);
    }

    // called by reflection
    static StreamingJobManager newInstance(KylinConfig config, String project) {
        return new StreamingJobManager(config, project);
    }

    private StreamingJobManager(KylinConfig config, String project) {
        this.project = project;
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.STREAMING_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<StreamingJobMeta>(getStore(), resourceRootPath, "", StreamingJobMeta.class) {
            @Override
            protected StreamingJobMeta initEntityAfterReload(StreamingJobMeta entity, String resourceName) {
                entity.setProject(project);
                return entity;
            }
        };
    }

    public StreamingJobMeta getStreamingJobByUuid(String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            return null;
        }
        return crud.get(uuid);
    }

    public void createStreamingJob(NDataModel model) {
        createStreamingJob(model, JobTypeEnum.STREAMING_BUILD);
        createStreamingJob(model, JobTypeEnum.STREAMING_MERGE);
    }

    public void createStreamingJob(NDataModel model, JobTypeEnum jobType) {
        StreamingJobMeta job = StreamingJobMeta.create(model, JobStatusEnum.STOPPED, jobType);
        crud.save(job);
    }

    public StreamingJobMeta copy(StreamingJobMeta jobMeta) {
        return crud.copyBySerialization(jobMeta);
    }

    public StreamingJobMeta updateStreamingJob(String jobId, NStreamingJobUpdater updater) {
        StreamingJobMeta cached = getStreamingJobByUuid(jobId);
        if (cached == null) {
            return null;
        }
        StreamingJobMeta copy = copy(cached);
        updater.modify(copy);
        return crud.save(copy);
    }

    public void deleteStreamingJob(String uuid) {
        StreamingJobMeta job = getStreamingJobByUuid(uuid);
        if (job == null) {
            log.warn("Dropping streaming job {} doesn't exists", uuid);
            return;
        }
        log.info("deleteStreamingJob:" + uuid);
        crud.delete(uuid);
    }

    public List<StreamingJobMeta> listAllStreamingJobMeta() {
        return crud.listAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

    public interface NStreamingJobUpdater {
        void modify(StreamingJobMeta copyForWrite);
    }

}
