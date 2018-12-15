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

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import lombok.val;

/**
 */
public class NExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);
    private static final Serializer<ExecutableOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<>(
            ExecutableOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(NExecutableDao.class);
    private static final String CREATE_TIME = "createTime";

    public static NExecutableDao getInstance(KylinConfig config, String project) {
        return config.getManager(project, NExecutableDao.class);
    }

    // called by reflection
    static NExecutableDao newInstance(KylinConfig config, String project) {
        return new NExecutableDao(config, project);
    }
    // ============================================================================

    private ResourceStore store;
    private String project;

    private NExecutableDao(KylinConfig config, String project) {
        logger.trace("Using metadata url: {}", config);
        this.store = ResourceStore.getKylinMetaStore(config);
        this.project = project;
    }

    private String pathOfJob(ExecutablePO job) {
        return pathOfJob(job.getUuid());
    }

    private String pathOfJob(String uuid) {
        return "/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    private String pathOfOutput(String uuid) {
        return "/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }

    private ExecutablePO readJobResource(String path) {
        return store.getResource(path, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, ExecutablePO job) {
        store.checkAndPutResource(path, job, JOB_SERIALIZER);
    }

    private ExecutableOutputPO readOutputResource(String path) {
        return store.getResource(path, JOB_OUTPUT_SERIALIZER);
    }

    private void writeOutputResource(String path, ExecutableOutputPO output) {
        store.checkAndPutResource(path, output, JOB_OUTPUT_SERIALIZER);
    }

    public List<ExecutableOutputPO> getJobOutputs() {
        return store.getAllResources("/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, JOB_OUTPUT_SERIALIZER);
    }

    public List<ExecutablePO> getJobs() {
        return store.getAllResources("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT, JOB_SERIALIZER);
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) {
        return store.getAllResources("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT, timeStart, timeEndExclusive,
                JOB_SERIALIZER);
    }

    public List<String> getJobPathes() {
        NavigableSet<String> resources = store.listResources("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT);
        if (resources == null) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(resources);
    }

    public ExecutablePO getJob(String path) {
        return readJobResource(path);
    }

    public ExecutablePO getJobByUuid(String uuid) {
        return readJobResource(pathOfJob(uuid));
    }

    public ExecutablePO addJob(ExecutablePO job) {
        if (getJobByUuid(job.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
        }
        writeJobResource(pathOfJob(job), job);
        return job;
    }

    public void deleteJob(String uuid) {
        store.deleteResource(pathOfJob(uuid));
    }

    public ExecutableOutputPO getOutputPO(String path) {
        ExecutableOutputPO result = readOutputResource(path);
        if (result == null) {
            result = new ExecutableOutputPO();
            result.setUuid(path.substring(path.lastIndexOf("/") + 1));
            return result;
        }
        return result;
    }

    public void addOutputPO(ExecutableOutputPO output) {
        output.setLastModified(0);
        val info = output.getInfo();
        info.put(CREATE_TIME, "" + System.currentTimeMillis());
        writeOutputResource(pathOfOutput(output.getUuid()), output);
    }

    public void updateOutputPO(ExecutableOutputPO output) {
        writeOutputResource(pathOfOutput(output.getUuid()), output);
    }

    //TODO why no one call this?
    public void deleteOutputPO(String uuid) {
        store.deleteResource(pathOfOutput(uuid));
    }
}
