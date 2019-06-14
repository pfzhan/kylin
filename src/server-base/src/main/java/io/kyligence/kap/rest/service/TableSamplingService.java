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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("tableSamplingService")
public class TableSamplingService extends BasicService {

    @Transaction(project = 1)
    public void sampling(Set<String> tables, String project, int rows) {
        NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getConfig(), project);

        val existingJobs = collectRunningSamplingJobs(tables, project);
        tables.forEach(table -> {
            // if existing a related job, discard it
            if (existingJobs.containsKey(table)) {
                execMgr.discardJob(existingJobs.get(table).getId());
            }

            val tableDesc = tableMgr.getTableDesc(table);
            val samplingJob = NTableSamplingJob.create(tableDesc, project, getUsername(), rows);
            execMgr.addJob(NExecutableManager.toPO(samplingJob, project));
        });
    }

    public boolean hasSamplingJob(String project, String table) {
        return !collectRunningSamplingJobs(Sets.newHashSet(table), project).isEmpty();
    }

    private Map<String, AbstractExecutable> collectRunningSamplingJobs(Set<String> tables, String project) {
        final List<AbstractExecutable> jobs = NExecutableManager.getInstance(getConfig(), project) //
                .getAllExecutables().stream() //
                .filter(executable -> executable instanceof NTableSamplingJob) //
                .filter(job -> !job.getStatus().isFinalState()) //
                .filter(job -> tables.contains(job.getTargetSubject())) //
                .collect(Collectors.toList());

        Map<String, AbstractExecutable> map = Maps.newHashMap();
        jobs.forEach(job -> map.put(job.getTargetSubject(), job));
        return map;
    }
}
