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

package io.kyligence.kap.job.dao;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class NExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<ExecutablePO>(ExecutablePO.class);
    private static final Serializer<ExecutableOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<ExecutableOutputPO>(
            ExecutableOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(ExecutableDao.class);

    public static NExecutableDao getInstance(KylinConfig config) {
        return config.getManager(NExecutableDao.class);
    }

    // called by reflection
    static NExecutableDao newInstance(KylinConfig config) throws IOException {
        return new NExecutableDao(config);
    }

    // ============================================================================

    private ResourceStore store;

    private NExecutableDao(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.store = ResourceStore.getKylinMetaStore(config);
    }

    private String pathOfJob(ExecutablePO job) {
        return pathOfJob(job.getUuid(), job.getProject());
    }

    public static String pathOfJob(String uuid, String project) {
        return "/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    public static String pathOfJobOutput(String uuid, String project) {
        return "/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }

    private ExecutablePO readJobResource(String path) throws IOException {
        return store.getResource(path, ExecutablePO.class, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, ExecutablePO job) throws IOException {
        store.putResource(path, job, JOB_SERIALIZER);
    }

    private ExecutableOutputPO readJobOutputResource(String path) throws IOException {
        return store.getResource(path, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
    }

    private long writeJobOutputResource(String path, ExecutableOutputPO output) throws IOException {
        return store.putResource(path, output, JOB_OUTPUT_SERIALIZER);
    }

    public List<ExecutableOutputPO> getJobOutputs(String project) throws PersistentException {
        try {
            return store.getAllResources("/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT,
                    ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    //    public List<ExecutableOutputPO> getJobOutputs() throws PersistentException {
    //        try {
    //            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, ExecutableOutputPO.class,
    //                    JOB_OUTPUT_SERIALIZER);
    //        } catch (IOException e) {
    //            logger.error("error get all Jobs:", e);
    //            throw new PersistentException(e);
    //        }
    //    }

    public List<ExecutableOutputPO> getJobOutputs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, timeStart, timeEndExclusive,
                    ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutablePO> getJobs(String project) throws PersistentException {
        try {
            return store.getAllResources("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT, ExecutablePO.class,
                    JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, timeStart, timeEndExclusive,
                    ExecutablePO.class, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<String> getJobPathes(String project) throws PersistentException {
        try {
            NavigableSet<String> resources = store.listResources("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT);
            if (resources == null) {
                return Collections.emptyList();
            }
            return Lists.newArrayList(resources);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO getJob(String path) throws PersistentException {
        try {
            return readJobResource(path);
        } catch (IOException e) {
            logger.error("error get job:" + path, e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO getJob(String uuid, String project) throws PersistentException {
        try {
            return readJobResource(pathOfJob(uuid, project));
        } catch (IOException e) {
            logger.error("error get job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO addJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid(), job.getProject()) != null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
            }
            writeJobResource(pathOfJob(job), job);
            return job;
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJob(String uuid, String project) throws PersistentException {
        try {
            store.deleteResource(pathOfJob(uuid, project));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public ExecutableOutputPO getJobOutput(String path) throws PersistentException {
        try {
            ExecutableOutputPO result = readJobOutputResource(path);
            if (result == null) {
                result = new ExecutableOutputPO();
                result.setUuid(path.substring(path.lastIndexOf("/") + 1));
                return result;
            }
            return result;
        } catch (IOException e) {
            logger.error("error get job output:" + path, e);
            throw new PersistentException(e);
        }
    }

    public void addJobOutput(ExecutableOutputPO output, String project) throws PersistentException {
        try {
            output.setLastModified(0);
            writeJobOutputResource(pathOfJobOutput(output.getUuid(), project), output);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void updateJobOutput(ExecutableOutputPO output, String project) throws PersistentException {
        try {
            final long ts = writeJobOutputResource(pathOfJobOutput(output.getUuid(), project), output);
            output.setLastModified(ts);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJobOutput(String uuid, String project) throws PersistentException {
        try {
            store.deleteResource(pathOfJobOutput(uuid, project));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }
}
