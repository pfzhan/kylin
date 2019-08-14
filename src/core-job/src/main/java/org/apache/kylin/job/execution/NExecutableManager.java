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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

/**
 *
 */
public class NExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(NExecutableManager.class);
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";

    private static final int CMD_EXEC_TIMEOUT_SEC = 5;
    private static final int PROCESS_TERM_TIMEOUT_SEC = 5;

    private static final String SIGKILL = "-9";
    private static final String SIGTERM = "-15";
    private static final String KILL_CHILD_PROCESS = "kill-child-process.sh";

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
        result.setTargetModel(executable.getTargetSubject());
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

    // only for test
    public void addJob(AbstractExecutable executable) {
        val po = toPO(executable, project);
        addJob(po);
    }

    public void addJob(ExecutablePO executablePO) {
        addJobOutput(executablePO);
        executableDao.addJob(executablePO);

        NMetricsGroup.counterInc(NMetricsName.JOB, NMetricsCategory.PROJECT,
                executablePO.getParams().get(NBatchConstants.P_PROJECT_NAME));
        // dispatch job-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv())
            SchedulerEventBusFactory.getInstance(config).postWithLimit(new JobReadyNotifier(project));
        else
            UnitOfWork.get().doAfterUnit(
                    () -> SchedulerEventBusFactory.getInstance(config).postWithLimit(new JobReadyNotifier(project)));
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
        try {
            return fromPO(executablePO);
        } catch (Exception e) {
            logger.error(PARSE_ERROR_MSG, e);
            return null;
        }
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

    /**
     * get job output from hdfs json file;
     * if json file contains logPath,
     * the logPath is spark driver log hdfs path(*.json.log), read sample data from log file.
     *
     * @param jobId
     * @return
     */
    public Output getOutputFromHDFSByJobId(String jobId) {
        ExecutableOutputPO jobOutput = getJobOutputFromHDFS(
                KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId));
        assertOutputNotNull(jobOutput, KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId));

        if (Objects.nonNull(jobOutput.getLogPath())) {
            jobOutput.setContent(getSampleDataFromHDFS(jobOutput.getLogPath(), 100));
        }
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
            } catch (Exception e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public long countByModelAndStatus(String model, Predicate<ExecutableState> predicate) {
        return countByModelAndStatus(model, predicate, null);
    }

    public long countByModelAndStatus(String model, Predicate<ExecutableState> predicate, JobTypeEnum jobType) {
        return getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model)) //
                .filter(e -> predicate.apply(e.getStatus()))
                .filter(e -> (jobType == null || jobType.equals(e.getJobType()))).count();
    }

    public Map<String, List<String>> getModelExecutables(Set<String> models, Predicate<ExecutableState> predicate) {
        Map<String, List<String>> result = getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> models.contains(e.getTargetSubject())) //
                .filter(e -> predicate.apply(e.getStatus()))
                .collect(Collectors.toMap(AbstractExecutable::getTargetSubject,
                        executable -> Lists.newArrayList(executable.getId()), (one, other) -> {
                            one.addAll(other);
                            return one;
                        }));
        return result;
    }

    public List<AbstractExecutable> getExecutablesByStatus(List<String> jobIds, ExecutableState status) {

        val executables = getAllExecutables();
        val resultExecutables = new ArrayList<AbstractExecutable>();
        if (CollectionUtils.isNotEmpty(jobIds)) {
            resultExecutables
                    .addAll(executables.stream().filter(t -> jobIds.contains(t.getId())).collect(Collectors.toList()));
        } else {
            resultExecutables.addAll(executables);
        }
        if (Objects.nonNull(status)) {
            return resultExecutables.stream().filter(t -> t.getStatus().equals(status)).collect(Collectors.toList());
        } else {
            return resultExecutables;
        }
    }

    public List<AbstractExecutable> getExecutablesByStatus(ExecutableState status) {

        val executables = getAllExecutables();

        if (Objects.nonNull(status)) {
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
            } catch (Exception e) {
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
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus().isNotProgressing())
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
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
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
        return getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model))
                .filter(executable -> executable.getCreateTime() > job.getCreateTime()).count();
    }

    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
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

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output) {
        val jobId = extractJobId(taskOrJobId);
        executableDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput;
            jobOutput = (Objects.equals(taskOrJobId, jobId)) ? job.getOutput()
                    : job.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                            .map(ExecutablePO::getOutput).orElse(null);
            assertOutputNotNull(jobOutput, taskOrJobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    logger.warn(
                            "[UNEXPECTED_THINGS_HAPPENED] wrong job state transfer! There is no valid state transfer from: {} to: {}, job id: {}",
                            oldStatus, newStatus, taskOrJobId);
                    throw new IllegalStateTranferException(
                            "UNEXPECTED_THINGS_HAPPENED, job " + taskOrJobId + " couldn't be continued.");
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

            if (needDestroyProcess(oldStatus, newStatus)) {
                logger.debug("need kill {}, from {} to {}", taskOrJobId, oldStatus, newStatus);
                // kill spark-submit process
                val context = UnitOfWork.get();
                context.doAfterUnit(() -> destroyProcess(taskOrJobId));
            }
            return true;
        });
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
        Process originProc = JobProcessContext.getProcess(jobId);
        if (originProc != null && originProc.isAlive()) {
            try {
                final int ppid = getPid(originProc);
                logger.info("start to destroy process {} of job {}", ppid, jobId);
                //build cmd template
                StringBuilder sb = new StringBuilder("bash ");
                sb.append(Paths.get(KylinConfig.getKylinHome(), "sbin", KILL_CHILD_PROCESS));
                sb.append(" %s ").append(ppid);
                final String template = sb.toString();
                //SIGTERM
                final String termCmd = String.format(template, SIGTERM);
                Process termProc = Runtime.getRuntime().exec(termCmd);
                if (termProc.waitFor(CMD_EXEC_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    logger.info("try to destroy process {} of job {} by SIGTERM, exec cmd '{}', exitValue : {}", ppid,
                            jobId, termCmd, termProc.exitValue());
                    if (originProc.waitFor(PROCESS_TERM_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                        logger.info("destroy process {} of job {} successfully in {}s", ppid, jobId,
                                PROCESS_TERM_TIMEOUT_SEC);
                        return;
                    }
                    logger.info("destroy process {} of job {} by SIGTERM exceed {}s", ppid, jobId,
                            PROCESS_TERM_TIMEOUT_SEC);
                }
                //SIGKILL
                final String killCmd = String.format(template, SIGKILL);
                Process killProc = Runtime.getRuntime().exec(killCmd);
                if (killProc.waitFor(CMD_EXEC_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    logger.info("try to destroy process {} of job {} by SIGKILL, exec cmd '{}', exitValue : {}", ppid,
                            jobId, killCmd, killProc.exitValue());
                    if (originProc.waitFor(PROCESS_TERM_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                        logger.info("destroy process {} of job {} successfully in {}s", ppid, jobId,
                                PROCESS_TERM_TIMEOUT_SEC);
                        return;
                    }
                    logger.info("destroy process {} of job {} by SIGKILL exceed {}s", ppid, jobId,
                            PROCESS_TERM_TIMEOUT_SEC);
                }

                //generally, code executing wouldn't reach here
                logger.warn("destroy process {} of job {} exceed {}s.", ppid, jobId, CMD_EXEC_TIMEOUT_SEC);
            } catch (Exception e) {
                logger.error("destroy process of job {} failed.", jobId, e);
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
        Preconditions.checkArgument(StringUtils.isNotEmpty(type),
                "Cannot parse this job: " + executablePO.getId() + ", the type is empty");
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            result.setProject(project);
            result.setParams(executablePO.getParams());
            result.setJobType(executablePO.getJobType());
            result.setTargetSubject(executablePO.getTargetModel());
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

    static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    private boolean needDestroyProcess(ExecutableState from, ExecutableState to) {
        if (from != ExecutableState.RUNNING || to == null) {
            return false;
        }
        return to == ExecutableState.PAUSED || to == ExecutableState.READY || to == ExecutableState.DISCARDED;
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

    /**
     * get sample data from hdfs log file.
     * specified the lines, will get the first num lines and last num lines.
     *
     * @param resPath
     * @return
     */
    public String getSampleDataFromHDFS(String resPath, final int nLines) {
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getFileSystem(path);
            if (!fs.exists(path)) {
                return null;
            }

            FileStatus fileStatus = fs.getFileStatus(path);
            try (FSDataInputStream din = fs.open(path);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(din))) {

                String line;
                StringBuilder sampleData = new StringBuilder();
                for (int i = 0; i < nLines && (line = reader.readLine()) != null; i++) {
                    if (sampleData.length() > 0) {
                        sampleData.append('\n');
                    }
                    sampleData.append(line);
                }

                int offset = sampleData.toString().getBytes().length + 1;
                if (offset < fileStatus.getLen()) {
                    sampleData.append("\n================================================================\n");
                    sampleData.append(tailHdfsFileInputStream(din, offset, fileStatus.getLen(), nLines));
                }
                return sampleData.toString();
            }
        } catch (IOException e) {
            logger.error("get sample data from hdfs log file [{}] failed!", resPath, e);
            return null;
        }
    }

    /**
     * get the last N_LINES lines from the end of hdfs file input stream;
     * reference: https://olapio.atlassian.net/wiki/spaces/PD/pages/1306918958
     *
     * @param hdfsDin
     * @param startPos
     * @param endPos
     * @param nLines
     * @return
     * @throws IOException
     */
    private String tailHdfsFileInputStream(FSDataInputStream hdfsDin, final long startPos, final long endPos,
            final int nLines) throws IOException {
        Preconditions.checkNotNull(hdfsDin);
        Preconditions.checkArgument(startPos < endPos && startPos >= 0);
        Preconditions.checkArgument(nLines >= 0);

        Deque<String> deque = new ArrayDeque<>();
        int buffSize = 8192;
        byte[] byteBuf = new byte[buffSize];

        long pos = endPos;

        // cause by log last char is \n
        hdfsDin.seek(pos - 1);
        int lastChar = hdfsDin.read();
        if ('\n' == lastChar) {
            pos--;
        }

        int bytesRead = (int) ((pos - startPos) % buffSize);
        if (bytesRead == 0) {
            bytesRead = buffSize;
        }

        pos -= bytesRead;
        int lines = nLines;
        while (lines > 0 && pos >= startPos) {
            bytesRead = hdfsDin.read(pos, byteBuf, 0, bytesRead);

            int last = bytesRead;
            for (int i = bytesRead - 1; i >= 0 && lines > 0; i--) {
                if (byteBuf[i] == '\n') {
                    deque.push(new String(byteBuf, i, last - i, StandardCharsets.UTF_8));
                    lines--;
                    last = i;
                }
            }

            if (lines > 0 && last > 0) {
                deque.push(new String(byteBuf, 0, last, StandardCharsets.UTF_8));
            }

            bytesRead = buffSize;
            pos -= bytesRead;
        }

        StringBuilder sb = new StringBuilder();
        while (!deque.isEmpty()) {
            sb.append(deque.pop());
        }

        return sb.length() > 0 && sb.charAt(0) == '\n' ? sb.substring(1) : sb.toString();
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
    }
}
