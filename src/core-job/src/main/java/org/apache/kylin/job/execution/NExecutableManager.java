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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.common.scheduler.JobCreatedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.JobProcessContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

/**
 *
 */
public class NExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(NExecutableManager.class);
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";

    public static NExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, NExecutableManager.class);
    }

    // called by reflection
    static NExecutableManager newInstance(KylinConfig config, String project) {
        return new NExecutableManager(config, project);
    }

    static NExecutableManager newInstance(KylinConfig config) {
        return new NExecutableManager(config, null);
    }

    // ============================================================================

    private final KylinConfig config;
    private String project;
    private final NExecutableDao executableDao;

    private NExecutableManager(KylinConfig config, String project) {
        logger.trace("Using metadata url: {}", config);
        this.config = config;
        this.project = project;
        this.executableDao = NExecutableDao.getInstance(config, project);
    }

    public static ExecutablePO toPO(AbstractExecutable executable, String project) {
        ExecutablePO result = new ExecutablePO();
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        result.setJobType(executable.getJobType());
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
        addJobOutput(executablePO);
        executableDao.addJob(executablePO);

        // dispatch job-created message out
        SchedulerEventBusFactory.getInstance(config).postWithLimit(new JobCreatedNotifier(project));
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executable.setOutput(executableOutputPO);
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
        if (!status.equals(ExecutableState.SUCCEED) && !status.equals(ExecutableState.DISCARDED)
                && !status.equals(ExecutableState.SUICIDAL)) {
            throw new IllegalStateException(
                    "Cannot drop running job " + executable.getDisplayName() + ", please discard it first.");
        }
        executableDao.deleteJob(jobId);
    }

    public AbstractExecutable getJob(String id) {
        if (id == null) {
            return null;
        }
        ExecutablePO executablePO = executableDao.getJobByUuid(id);
        if (executablePO == null) {
            return null;
        }
        return fromPO(executablePO);
    }

    public long getCreateTime(String id) {
        ExecutablePO executablePO = executableDao.getJobByUuid(extractJobId(id));
        if (executablePO == null) {
            return 0L;
        }
        return executablePO.getOutput().getCreateTime();
    }

    public Output getOutput(String id) {
        val jobOutput = getJobOutput(id);
        assertOutputNotNull(jobOutput, id);
        return parseOutput(jobOutput);
    }

    public Output getOutputFromHDFSByJobId(String jobId) {
        ExecutableOutputPO jobOutput = getJobOutputFromHDFS(
                KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId));
        assertOutputNotNull(jobOutput, KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId));
        return parseOutput(jobOutput);
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        result.setStartTime(jobOutput.getStartTime());
        result.setEndTime(jobOutput.getEndTime());
        result.setWaitTime(jobOutput.getWaitTime());
        result.setCreateTime(jobOutput.getCreateTime());
        return result;
    }

    public List<AbstractExecutable> getAllExecutables() {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs()) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public long countByModelAndStatus(String model, Set<ExecutableState> status) {
        return countByModelAndStatus(model, status, null);
    }

    public long countByModelAndStatus(String model, Set<ExecutableState> status, JobTypeEnum jobType) {
        return getAllExecutables().stream().filter(e -> e.getTargetModel().equals(model))
                .filter(e -> status.contains(e.getStatus()))
                .filter(e -> (jobType == null || jobType.equals(e.getJobType()))).count();
    }

    public Map<String, List<String>> getModelExecutables(Set<String> models, Set<ExecutableState> status) {
        Map<String, List<String>> result = getAllExecutables().stream().filter(e -> models.contains(e.getTargetModel()))
                .filter(e -> status.contains(e.getStatus()))
                .collect(Collectors.toMap(AbstractExecutable::getTargetModel,
                        executable -> Lists.newArrayList(executable.getId()), (one, other) -> {
                            one.addAll(other);
                            return one;
                        }));
        return result;
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

    public List<AbstractExecutable> getExecutablesByStatus(ExecutableState status) {

        val executables = getAllExecutables();

        if (status != null) {
            return executables.stream().filter(t -> t.getStatus().equals(status)).collect(Collectors.toList());
        } else {
            return executables;
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error(PARSE_ERROR_MSG, e);
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
        for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = parseToAbstract(po, expectedClass);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public List<String> getJobs() {
        return Lists.newArrayList(executableDao.getJobIds());
    }

    public void resumeAllRunningJobs() {
        val jobs = executableDao.getJobs();
        for (ExecutablePO executablePO : jobs) {
            executableDao.updateJob(executablePO.getUuid(), this::resumeRunningJob);
        }
    }

    private boolean resumeRunningJob(ExecutablePO po) {
        boolean result = false;
        if (po.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
            po.getOutput().setStatus(ExecutableState.READY.toString());
            po.getOutput().addEndTime(System.currentTimeMillis());
            result = true;
        }
        for (ExecutablePO task : Optional.ofNullable(po.getTasks()).orElse(Lists.newArrayList())) {
            result = resumeRunningJob(task) || result;
        }
        return result;
    }

    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.READY)
                    .filter(task -> (task.getStatus() == ExecutableState.ERROR
                            || task.getStatus() == ExecutableState.PAUSED))
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
        }
        updateJobOutput(jobId, ExecutableState.READY);
    }

    public void restartJob(String jobId) {
        updateJobReady(jobId);
        resetJobTime(jobId);
    }

    private void updateJobReady(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.READY)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
        }
        updateJobOutput(jobId, ExecutableState.READY);
    }

    private void resetJobTime(String jobId) {
        executableDao.updateJob(jobId, job -> {
            job.getOutput().resetTime();
            job.getTasks().forEach(task -> task.getOutput().resetTime());
            return true;
        });
    }

    public long countCuttingInJobByModel(String model, AbstractExecutable job) {
        return getAllExecutables().stream().filter(e -> e.getTargetModel().equals(model))
                .filter(executable -> executable.getCreateTime() > job.getCreateTime()).count();
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
                    updateJobOutput(task.getId(), ExecutableState.DISCARDED);
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.DISCARDED);
    }

    public void pauseJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        updateJobOutput(jobId, ExecutableState.PAUSED);
        // pauseJob may happen when the job has not been scheduled
        // then call this hook after updateJobOutput
        job.onExecuteStopHook();
    }

    ExecutableOutputPO getJobOutput(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = executableDao.getJobByUuid(jobId);
        final ExecutableOutputPO jobOutput;
        if (executablePO == null) {
            jobOutput = new ExecutableOutputPO();
        } else if (Objects.equals(taskOrJobId, jobId)) {
            jobOutput = executablePO.getOutput();
        } else {
            jobOutput = executablePO.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                    .map(ExecutablePO::getOutput).orElse(null);
        }
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,  Set<String> removeInfo,
            String output) {
        val jobId = extractJobId(taskOrJobId);
        executableDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput;
            jobOutput = (Objects.equals(taskOrJobId, jobId)) ? job.getOutput() : job.getTasks()
                    .stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                    .map(ExecutablePO::getOutput).orElse(null);
            assertOutputNotNull(jobOutput, taskOrJobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    throw new IllegalStateTranferException("There is no valid state transfer from: " + oldStatus
                            + " to: " + newStatus + ", job id: " + taskOrJobId);
                }
                jobOutput.setStatus(String.valueOf(newStatus));
                updateJobStatus(jobOutput, oldStatus, newStatus);
            }
            Map<String, String> info = Maps.newHashMap(jobOutput.getInfo());
            Optional.ofNullable(updateInfo).ifPresent(info::putAll);
            Optional.ofNullable(removeInfo).ifPresent(set -> set.forEach(info::remove));
            jobOutput.setInfo(info);
            Optional.ofNullable(output).ifPresent(jobOutput::setContent);
            logger.info("Job id: {} from {} to {}", taskOrJobId, oldStatus, newStatus);
            return true;
        });
        if (ExecutableState.PAUSED.equals(newStatus)) {
            // kill spark-submit process
            destroyProcess(taskOrJobId);
        }
    }

    private void updateJobStatus(ExecutableOutputPO jobOutput, ExecutableState oldStatus, ExecutableState newStatus) {
        long time = System.currentTimeMillis();
        if (oldStatus == ExecutableState.RUNNING) {
            jobOutput.addEndTime(time);
        } else if (newStatus == ExecutableState.RUNNING) {
            jobOutput.addStartTime(time);
        } else if (newStatus == ExecutableState.SUICIDAL || newStatus == ExecutableState.DISCARDED) {
            jobOutput.addStartTime(time);
            jobOutput.addEndTime(time);
        }
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus) {
        updateJobOutput(taskOrJobId, newStatus, null, null, null);
    }

    public void destroyProcess(String jobId) {
        // in ut env, there is no process for job, just do nothing
        if (!config.isUTEnv()) {
            Process process = JobProcessContext.getProcess(jobId);
            if (process != null && process.isAlive()) {
                String cmd = "";
                try {
                    int pid = getPid(process);
                    cmd = String.format("pkill -P %d", pid);
                    logger.info("destroyProcess pid {} of job {} with cmd '{}'", pid, jobId, cmd);
                    Process proc = Runtime.getRuntime().exec(cmd);
                    int exitValue = proc.waitFor();
                    logger.info("exec cmd '{}', exitValue : {}", cmd, exitValue);
                } catch (Exception e) {
                    logger.error("exec cmd : '{}', error : {}", cmd, e.getMessage(), e);
                }
            }
        }
    }

    private int getPid(Process process) throws IllegalAccessException, NoSuchFieldException {
        String className = process.getClass().getName();
        Preconditions.checkState(className.equals("java.lang.UNIXProcess"));
        Field f = process.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        return f.getInt(process);
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
            result.setProject(project);
            result.setParams(executablePO.getParams());
            result.setJobType(executablePO.getJobType());
            result.setDataRangeStart(executablePO.getDataRangeStart());
            result.setDataRangeEnd(executablePO.getDataRangeEnd());
            result.setTargetModel(executablePO.getTargetModel());
            result.setTargetSegments(executablePO.getTargetSegments());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    ((ChainedExecutable) result).addTask(fromPO(subTask));
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot parse this job: " + executablePO.getId(), e);
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
            throw new IllegalStateException("Cannot parse this job:" + executablePO.getId(), e);
        }
    }

    private static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    public void updateJobOutputToHDFS(String resPath, ExecutableOutputPO obj) {
        DataOutputStream dout = null;
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getFileSystem(path);
            dout = fs.create(path, true);
            JsonUtil.writeValue(dout, obj);
        } catch (Exception e) {
            // the operation to update output to hdfs failed, next task should not be interrupted.
            logger.error("update job output [{}] to HDFS failed.", resPath, e);
        } finally {
            IOUtils.closeQuietly(dout);
        }
    }

    public ExecutableOutputPO getJobOutputFromHDFS(String resPath) {
        DataInputStream din = null;
        try {
            val path = new Path(resPath);
            FileSystem fs = HadoopUtil.getFileSystem(path);
            if (!fs.exists(path)) {
                val executableOutputPO = new ExecutableOutputPO();
                executableOutputPO.setContent("job output not found, please check kylin.log");
                return executableOutputPO;
            }

            din = fs.open(path);
            return JsonUtil.readValue(din, ExecutableOutputPO.class);
        } catch (Exception e) {
            // If the output file on hdfs is corrupt, give an empty output
            logger.error("get job output [{}] from HDFS failed.", resPath, e);
            val executableOutputPO = new ExecutableOutputPO();
            executableOutputPO.setContent("job output broken, please check kylin.log");
            return executableOutputPO;
        } finally {
            IOUtils.closeQuietly(din);
        }
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
    }
}
