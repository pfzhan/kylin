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

import static org.apache.kylin.job.execution.AbstractExecutable.RUNTIME_INFO;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

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

    public static ExecutablePO toPO(AbstractExecutable executable, String project) {
        ExecutablePO result = new ExecutablePO();
        result.setProject(project);
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        result.setJobType(executable.getName());
        result.setDataRangeStart(executable.getDataRangeStart());
        result.setDataRangeEnd(executable.getDataRangeEnd());
        result.setTargetModel(executable.getTargetModel());
        result.setTargetSegments(executable.getTargetSegments());
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
                tasks.add(toPO(task, project));
            }
            result.setTasks(tasks);
        }
        return result;
    }

    public void addJob(AbstractExecutable executable) {
        executable.initConfig(config);
        val po = toPO(executable, project);
        addJob(po);
    }

    public void addJob(ExecutablePO executablePO) {
        executableDao.addJob(executablePO);
        addJobOutput(executablePO);
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setUuid(executable.getId());
        executableDao.addOutputPO(executableOutputPO, project);
        if (CollectionUtils.isEmpty(executable.getTasks())) {
            return;
        }
        for (ExecutablePO subTask : executable.getTasks()) {
            addJobOutput(subTask);
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        AbstractExecutable executable = getJob(jobId);
        ExecutableState status = executable.getStatus();
        if (!status.equals(ExecutableState.SUCCEED) && !status.equals(ExecutableState.DISCARDED)) {
            throw new IllegalStateException(
                    "Cannot drop running job " + executable.getName() + ", please discard it first.");
        }
        executableDao.deleteJob(jobId, project);
    }

    public AbstractExecutable getJobByPath(String path) {
        ExecutablePO executablePO = executableDao.getJob(path);
        if (executablePO == null) {
            return null;
        }
        executablePO.setProject(extractProject(path));
        return fromPO(executablePO);
    }

    public AbstractExecutable getJob(String id) {
        return getJobByPath("/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + id);
    }

    public Output getOutputByJobPath(String jobPath) {
        String project = extractProject(jobPath);
        String id = extractId(jobPath);
        return getOutputByPath(pathOfOutput(id, project));
    }

    public Output getOutput(String id) {
        return getOutputByPath("/" + project + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + id);
    }

    private Output getOutputByPath(String path) {
        final ExecutableOutputPO jobOutput = executableDao.getOutputPO(path);
        Preconditions.checkArgument(jobOutput != null, "there is no related output for job :" + path);
        return parseOutput(jobOutput);
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        return result;
    }

    public List<AbstractExecutable> getAllExecutables() {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs(project)) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error("error parsing one executabePO: ", e);
            }
        }
        return ret;
    }

    public long countByModelAndStatus(String model, Set<ExecutableState> status) {
        return getAllExecutables().stream().filter(e -> e.getTargetModel().equals(model))
                .filter(e -> status.contains(e.getStatus())).count();
    }

    public List<AbstractExecutable> getExecutablesByStatus(List<String> jobIds, String status) {

        val executables = getAllExecutables();
        val resultExecutables = new ArrayList<AbstractExecutable>();
        if (CollectionUtils.isNotEmpty(jobIds)) {
            resultExecutables
                    .addAll(executables.stream().filter(t -> jobIds.contains(t.getId())).collect(Collectors.toList()));
        } else {
            resultExecutables.addAll(executables);
        }
        if (StringUtils.isNotEmpty(status)) {
            return resultExecutables.stream().filter(t -> t.getStatus().equals(ExecutableState.valueOf(status)))
                    .collect(Collectors.toList());
        } else {
            return resultExecutables;
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs(project, timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error("error parsing one executabePO: ", e);
            }
        }
        return ret;
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
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs(project, timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = parseToAbstract(po, expectedClass);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error("error parsing one executabePO: ", e);
            }
        }
        return ret;
    }

    public List<String> getJobPathes(String project) {
        return Lists.newArrayList(executableDao.getJobPathes(project));
    }

    public List<String> getAllJobPathes() {
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        List<String> result = Lists.newArrayList();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            result.addAll(getJobPathes(prj.getName()));
        }
        return result;
    }

    public void resumeAllRunningJobs() {
        final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(project);
        for (ExecutableOutputPO executableOutputPO : jobOutputs) {
            if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                executableOutputPO.setStatus(ExecutableState.READY.toString());
                executableDao.updateOutputPO(executableOutputPO, project);
            }
        }
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

    public void pauseJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        updateJobOutput(jobId, ExecutableState.STOPPED, null, null);
    }

    public ExecutableOutputPO getJobOutput(String path) {
        return executableDao.getOutputPO(path);
    }

    public void updateJobOutput(String jobId, ExecutableState newStatus, Map<String, String> info, String output) {
        final ExecutableOutputPO jobOutput = executableDao.getOutputPO(pathOfOutput(jobId, project));
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
            if (jobOutput.getInfo() != null) {
                jobOutput.getInfo().putAll(info);
            } else {
                jobOutput.setInfo(info);
            }
        }
        if (output != null) {
            jobOutput.setContent(output);
        }
        executableDao.updateOutputPO(jobOutput, project);
        logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
    }

    public void forceKillJob(String jobId) {
        final ExecutableOutputPO jobOutput = executableDao.getOutputPO(pathOfOutput(jobId, project));
        jobOutput.setStatus(ExecutableState.ERROR.toString());
        List<ExecutablePO> tasks = executableDao.getJob(pathOfJob(jobId, project)).getTasks();

        for (ExecutablePO task : tasks) {
            if (executableDao.getOutputPO(pathOfJob(task.getId(), project)).getStatus().equals("SUCCEED")) {
                continue;
            } else if (executableDao.getOutputPO(pathOfJob(task.getId(), project)).getStatus().equals("RUNNING")) {
                updateJobOutput(task.getId(), ExecutableState.READY, Maps.<String, String> newHashMap(), "");
            }
            break;
        }
        executableDao.updateOutputPO(jobOutput, project);
    }

    private AbstractExecutable fromPO(ExecutablePO executablePO) {
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
            result.setName(executablePO.getJobType());
            result.setDataRangeStart(executablePO.getDataRangeStart());
            result.setDataRangeEnd(executablePO.getDataRangeEnd());
            result.setTargetModel(executablePO.getTargetModel());
            result.setTargetSegments(executablePO.getTargetSegments());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    subTask.setProject(executablePO.getProject());
                    ((ChainedExecutable) result).addTask(fromPO(subTask));
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot toPO this job:" + executablePO.getId(), e);
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
                        parseToTask = fromPO(subTask);
                    } catch (IllegalStateException e) {
                        parseToTask = parseToAbstract(subTask, DefaultChainedExecutable.class);
                    }
                    ((ChainedExecutable) result).addTask(parseToTask);
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot toPO this job:" + executablePO.getId(), e);
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
