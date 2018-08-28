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

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.constant.ExecutableConstants.MR_JOB_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;
import static org.apache.kylin.job.execution.AbstractExecutable.RUNTIME_INFO;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.cube.model.NDataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.NProjectManager;

/**
 */
public class NExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(NExecutableManager.class);

    public static NExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, NExecutableManager.class);
    }

    // called by reflection
    static NExecutableManager newInstance(KylinConfig config, String project) throws IOException {
        System.out.println();
        return new NExecutableManager(config, project);
    }

    static NExecutableManager newInstance(KylinConfig config) throws IOException {
        return new NExecutableManager(config, null);
    }

    // ============================================================================

    private final KylinConfig config;
    private String project;
    private final NExecutableDao executableDao;

    private NExecutableManager(KylinConfig config, String project) {
        logger.info("Using metadata url: " + config);
        this.config = config;
        this.project = project;
        this.executableDao = NExecutableDao.getInstance(config);
    }

    private static ExecutablePO parse(AbstractExecutable executable, String project) {
        ExecutablePO result = new ExecutablePO();
        result.setProject(project);
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        Map<String, Object> runTimeInfo = executable.getRunTimeInfo();
        if (runTimeInfo != null && runTimeInfo.size() > 0) {
            Set<NDataSegment> segments = (HashSet<NDataSegment>) runTimeInfo.get(RUNTIME_INFO);
            if (segments != null) {
                result.getSegments().addAll(segments);
            }
        }
        if (executable instanceof ChainedExecutable) {
            List<ExecutablePO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((ChainedExecutable) executable).getTasks()) {
                tasks.add(parse(task, project));
            }
            result.setTasks(tasks);
        }
        return result;
    }

    public void addJob(AbstractExecutable executable) {
        try {
            executable.initConfig(config);
            executableDao.addJob(parse(executable, project));
            addJobOutput(executable);
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    private void addJobOutput(AbstractExecutable executable) throws PersistentException {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setUuid(executable.getId());
        executableDao.addJobOutput(executableOutputPO, project);
        if (executable instanceof DefaultChainedExecutable) {
            for (AbstractExecutable subTask : ((DefaultChainedExecutable) executable).getTasks()) {
                addJobOutput(subTask);
            }
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        try {
            executableDao.deleteJob(jobId, project);
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + jobId, e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJobByPath(String path) {
        try {
            ExecutablePO executablePO = executableDao.getJob(path);
            executablePO.setProject(extractProject(path));
            return parseTo(executablePO);
        } catch (PersistentException e) {
            logger.error("fail to get job:" + path, e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJob(String id) {
        return getJobByPath("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + id);
    }

    public Output getOutputByJobPath(String jobPath) {
        String project = extractProject(jobPath);
        String id = extractId(jobPath);
        return getOutputByPath(pathOfOutput(id, project));
    }

    public Output getOutputByPath(String path) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(path);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job :" + path);
            return parseOutput(jobOutput);
        } catch (PersistentException e) {
            logger.error("fail to get job output:" + path, e);
            throw new RuntimeException(e);
        }
    }

    public Output getOutput(String id) {
        return getOutputByPath("/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + id);
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        return result;
    }

    public Map<String, Output> getAllOutputs() {
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        HashMap<String, Output> result = Maps.newHashMap();
        try {
            for (ProjectInstance projectInstance : prjMgr.listAllProjects()) {
                final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(projectInstance.getName());
                for (ExecutableOutputPO jobOutput : jobOutputs) {
                    result.put(jobOutput.getId(), parseOutput(jobOutput));
                }
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Output> getAllOutputs(long timeStartInMillis, long timeEndInMillis) {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(timeStartInMillis, timeEndInMillis);
            HashMap<String, Output> result = Maps.newHashMap();
            for (ExecutableOutputPO jobOutput : jobOutputs) {
                result.put(jobOutput.getId(), parseOutput(jobOutput));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables() {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs(project)) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Since ExecutableManager will instantiate all AbstractExecutable class by Class.forName(), but for each version release,
     * new classes are introduced, old classes are deprecated, renamed or removed. The Class.forName() will throw out
     * ClassNotFoundException. This API is used to retrieve the Executable Object list, not for calling the object method,
     * so we could just instance the parent common class instead of the concrete class. It will tolerate the class missing issue.
     *
     * @param timeStartInMillis
     * @param timeEndInMillis
     * @param expectedClass
     * @return
     */
    public List<AbstractExecutable> getAllAbstractExecutables(long timeStartInMillis, long timeEndInMillis,
            Class<? extends AbstractExecutable> expectedClass) {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
                try {
                    AbstractExecutable ae = parseToAbstract(po, expectedClass);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getAbstractExecutable(String uuid, Class<? extends AbstractExecutable> expectedClass) {
        try {
            return parseToAbstract(executableDao.getJob(uuid, project), expectedClass);
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllJobPathes() {
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        List<String> result = Lists.newArrayList();
        try {
            for (ProjectInstance prj : prjMgr.listAllProjects()) {
                result.addAll(executableDao.getJobPathes(prj.getName()));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("error get All Job Ids", e);
            throw new RuntimeException(e);
        }
    }

    public void updateAllRunningJobsToError() {
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        try {
            for (ProjectInstance projectInstance : prjMgr.listAllProjects()) {
                final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(projectInstance.getName());
                for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                    if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                        executableOutputPO.setStatus(ExecutableState.ERROR.toString());
                        executableDao.updateJobOutput(executableOutputPO, project);
                    }
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to ERROR", e);
            throw new RuntimeException(e);
        }
    }

    public void resumeAllRunningJobs() {
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        try {
            for (ProjectInstance projectInstance : prjMgr.listAllProjects()) {
                final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(projectInstance.getName());
                for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                    if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                        executableOutputPO.setStatus(ExecutableState.READY.toString());
                        executableDao.updateJobOutput(executableOutputPO, project);
                    }
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to READY", e);
            throw new RuntimeException(e);
        }
    }

    public void resumeRunningJobForce(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.RUNNING) {
                    updateJobOutput(task.getId(), ExecutableState.READY, null, null);
                    break;
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.READY, null, null);
    }

    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        Map<String, String> info = null;
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.ERROR || task.getStatus() == ExecutableState.STOPPED) {
                    updateJobOutput(task.getId(), ExecutableState.READY, null, null);
                    break;
                }
            }
            final long endTime = job.getEndTime();
            if (endTime != 0) {
                long interruptTime = System.currentTimeMillis() - endTime + job.getInterruptTime();
                info = Maps.newHashMap(getJobOutput(jobId).getInfo());
                info.put(AbstractExecutable.INTERRUPT_TIME, Long.toString(interruptTime));
                info.remove(AbstractExecutable.END_TIME);
            }
        }
        updateJobOutput(jobId, ExecutableState.READY, info, null);
    }

    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (!task.getStatus().isFinalState()) {
                    updateJobOutput(task.getId(), ExecutableState.DISCARDED, null, null);
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.DISCARDED, null, null);
    }

    public void rollbackJob(String jobId, String stepId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getId().compareTo(stepId) >= 0) {
                    logger.debug("rollback task : " + task);
                    updateJobOutput(task.getId(), ExecutableState.READY, Maps.<String, String> newHashMap(), "");
                }
            }
        }

        if (job.getStatus() == ExecutableState.SUCCEED) {
            updateJobOutput(job.getId(), ExecutableState.READY, null, null);
        }
    }

    public void pauseJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        updateJobOutput(jobId, ExecutableState.STOPPED, null, null);
    }

    public ExecutableOutputPO getJobOutput(String path) {
        try {
            return executableDao.getJobOutput(path);
        } catch (PersistentException e) {
            logger.error("Can't get output of Job " + path);
            throw new RuntimeException(e);
        }
    }

    public void updateJobOutput(String jobId, ExecutableState newStatus, Map<String, String> info, String output) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(pathOfOutput(jobId, project));
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + jobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    throw new IllegalStateTranferException("there is no valid state transfer from:" + oldStatus + " to:"
                            + newStatus + ", job id: " + jobId);
                }
                jobOutput.setStatus(newStatus.toString());
            }
            if (info != null) {
                jobOutput.setInfo(info);
            }
            if (output != null) {
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput, project);
            logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
        } catch (PersistentException e) {
            logger.error("error change job:" + jobId + " to " + newStatus);
            throw new RuntimeException(e);
        }
    }

    public void forceKillJob(String jobId) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(pathOfOutput(jobId, project));
            jobOutput.setStatus(ExecutableState.ERROR.toString());
            List<ExecutablePO> tasks = executableDao.getJob(pathOfJob(jobId, project)).getTasks();

            for (ExecutablePO task : tasks) {
                if (executableDao.getJobOutput(pathOfJob(task.getId(), project)).getStatus().equals("SUCCEED")) {
                    continue;
                } else if (executableDao.getJobOutput(pathOfJob(task.getId(), project)).getStatus().equals("RUNNING")) {
                    updateJobOutput(task.getId(), ExecutableState.READY, Maps.<String, String> newHashMap(), "");
                }
                break;
            }
            executableDao.updateJobOutput(jobOutput, project);
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    //for migration only
    //TODO delete when migration finished
    public void resetJobOutput(String jobId, ExecutableState state, String output) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(pathOfJob(jobId, project));
            jobOutput.setStatus(state.toString());
            if (output != null) {
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput, project);
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    public void addJobInfo(String id, Map<String, String> info) {
        if (info == null) {
            return;
        }

        // post process
        if (info.containsKey(MR_JOB_ID) && !info.containsKey(ExecutableConstants.YARN_APP_ID)) {
            String jobId = info.get(MR_JOB_ID);
            if (jobId.startsWith("job_")) {
                info.put(YARN_APP_ID, jobId.replace("job_", "application_"));
            }
        }

        if (info.containsKey(YARN_APP_ID) && !StringUtils.isEmpty(config.getJobTrackingURLPattern())) {
            String pattern = config.getJobTrackingURLPattern();
            try {
                String newTrackingURL = String.format(pattern, info.get(YARN_APP_ID));
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: " + config.getJobTrackingURLPattern());
            }
        }

        try {
            ExecutableOutputPO output = executableDao.getJobOutput(pathOfOutput(id, project));
            Preconditions.checkArgument(output != null, "there is no related output for job id:" + id);
            output.getInfo().putAll(info);
            executableDao.updateJobOutput(output, project);
        } catch (PersistentException e) {
            logger.error("error update job info, id:" + id + "  info:" + info.toString());
            throw new RuntimeException(e);
        }
    }

    public void addJobInfo(String id, String key, String value) {
        Map<String, String> info = Maps.newHashMap();
        info.put(key, value);
        addJobInfo(id, info);
    }

    private AbstractExecutable parseTo(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.initConfig(config);
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            result.setProject(executablePO.getProject());
            result.setParams(executablePO.getParams());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    subTask.setProject(executablePO.getProject());
                    ((ChainedExecutable) result).addTask(parseTo(subTask));
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot parse this job:" + executablePO.getId(), e);
        }
    }

    private AbstractExecutable parseToAbstract(ExecutablePO executablePO,
            Class<? extends AbstractExecutable> expectedClass) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = null;
            try {
                clazz = ClassUtil.forName(type, AbstractExecutable.class);
            } catch (ClassNotFoundException e) {
                clazz = ClassUtil.forName(expectedClass.getName(), AbstractExecutable.class);
            }
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.initConfig(config);
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            result.setParams(executablePO.getParams());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    AbstractExecutable parseToTask = null;
                    try {
                        parseToTask = parseTo(subTask);
                    } catch (IllegalStateException e) {
                        parseToTask = parseToAbstract(subTask, DefaultChainedExecutable.class);
                    }
                    ((ChainedExecutable) result).addTask(parseToTask);
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot parse this job:" + executablePO.getId(), e);
        }
    }

    private String pathOfJob(String uuid, String project) {
        return "/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    private String pathOfOutput(String uuid, String project) {
        return "/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }

    private String extractProject(String path) {
        return path.split("/")[1];
    }

    public static String extractId(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }
}
