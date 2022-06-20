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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.NTableSamplingJob;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.manager.JobManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.aspect.Transaction;
import lombok.val;

//@Component("tableSamplingService")
public class TableSamplingService extends BasicService implements TableSamplingSupporter {

    private static final int MAX_SAMPLING_ROWS = 20_000_000;
    private static final int MIN_SAMPLING_ROWS = 10_000;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Override
    @Transaction(project = 1)
    public List<String> sampling(Set<String> tables, String project, int rows, int priority, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectWritePermission(project);
        ExecutableManager execMgr = ExecutableManager.getInstance(getConfig(), project);
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getConfig(), project);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);

        val existingJobs = collectRunningSamplingJobs(tables, project);
        List<String> jobIds = Lists.newArrayList();
        tables.forEach(table -> {
            // if existing a related job, discard it
            if (existingJobs.containsKey(table)) {
                execMgr.discardJob(existingJobs.get(table).getId());
            }

            JobManager.checkStorageQuota(project);
            val tableDesc = tableMgr.getTableDesc(table);
            val samplingJob = NTableSamplingJob.create(tableDesc, project, getUsername(), rows, priority, yarnQueue,
                    tag);
            jobIds.add(samplingJob.getId());
            execMgr.addJob(ExecutableManager.toPO(samplingJob, project));
            long startOfDay = TimeUtil.getDayStart(System.currentTimeMillis());
            jobStatisticsManager.updateStatistics(startOfDay, 0, 0, 1);
        });
        return jobIds;
    }

    public boolean hasSamplingJob(String project, String table) {
        aclEvaluate.checkProjectWritePermission(project);
        return !collectRunningSamplingJobs(Sets.newHashSet(table), project).isEmpty();
    }

    private Map<String, AbstractExecutable> collectRunningSamplingJobs(Set<String> tables, String project) {
        final List<AbstractExecutable> jobs = ExecutableManager
                .getInstance(KylinConfig.readSystemKylinConfig(), project).getAllJobs(0, Long.MAX_VALUE).stream()
                .filter(job -> !ExecutableState.valueOf(job.getOutput().getStatus()).isFinalState())
                .map(job -> getManager(ExecutableManager.class, job.getProject()).fromPO(job)) //
                .filter(NTableSamplingJob.class::isInstance) //
                .filter(job -> tables.contains(job.getTargetSubject())) //
                .collect(Collectors.toList());

        Map<String, AbstractExecutable> map = Maps.newHashMap();
        jobs.forEach(job -> map.put(job.getTargetSubject(), job));
        return map;
    }

    public static void checkSamplingRows(int rows) {
        if (rows > MAX_SAMPLING_ROWS || rows < MIN_SAMPLING_ROWS) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }
    }

    public static void checkSamplingTable(String tableName) {
        Message msg = MsgPicker.getMsg();
        if (tableName == null || StringUtils.isEmpty(tableName.trim())) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getFailedForNoSamplingTable());
        }

        if (tableName.contains(" ") || !tableName.contains(".") || tableName.split("\\.").length != 2) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getSamplingFailedForIllegalTableName());
        }
    }
}
