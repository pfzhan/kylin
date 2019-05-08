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

package io.kyligence.kap.engine.spark.job;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NTableSamplingJob extends DefaultChainedExecutable {

    private void setTableIdentity(String tableIdentity) {
        setParam(NBatchConstants.P_TABLE_NAME, tableIdentity);
    }

    public String getTableIdentity() {
        return getParam(NBatchConstants.P_TABLE_NAME);
    }

    @Override
    protected boolean needSuicide() {
        //todo
        return false;
    }

    public static NTableSamplingJob create(TableDesc tableDesc, String project, String submitter, int rows) {
        NTableSamplingJob job = new NTableSamplingJob();
        job.setId(UUID.randomUUID().toString());
        job.setSubmitter(submitter);
        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_SAMPLING_ROWS, String.valueOf(rows));
        job.setTableIdentity(tableDesc.getIdentity());
        job.setName(JobTypeEnum.TABLE_SAMPLING.toString());
        job.setProject(project);
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.addTask(SamplingStep.create(tableDesc, project, submitter, rows));
        return job;
    }

    public static class SamplingStep extends NSparkExecutable {
        private void setTableIdentity(String tableIdentity) {
            setParam(NBatchConstants.P_TABLE_NAME, tableIdentity);
        }

        private String getTableIdentity() {
            return getParam(NBatchConstants.P_TABLE_NAME);
        }

        private void setSamplingRows(int rows) {
            setParam(NBatchConstants.P_SAMPLING_ROWS, String.valueOf(rows));
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            ExecuteResult result = super.doWork(context);
            UnitOfWork.replaying.set(true);
            UnitOfWork.doInTransactionWithRetry(() -> {
                mergeRemoteMetaAfterSampling();
                return null;
            }, getProject());
            UnitOfWork.replaying.remove();
            return result;
        }

        private void mergeRemoteMetaAfterSampling() {
            try (val remoteStore = ExecutableUtils.getRemoteStore(getConfig(), this)) {
                val remoteConfig = remoteStore.getConfig();
                final NTableMetadataManager remoteTblMgr = NTableMetadataManager.getInstance(remoteConfig,
                        getProject());
                final NTableMetadataManager localTblMgr = NTableMetadataManager.getInstance(getConfig(), getProject());
                mergeAndUpdateTableExt(localTblMgr, remoteTblMgr, getTableIdentity());
            }
        }

        private void mergeAndUpdateTableExt(NTableMetadataManager localTblMgr, NTableMetadataManager remoteTblMgr,
                String tableName) {
            val localFactTblExt = localTblMgr.getOrCreateTableExt(tableName);
            val remoteFactTblExt = remoteTblMgr.getOrCreateTableExt(tableName);
            localTblMgr.mergeAndUpdateTableExt(localFactTblExt, remoteFactTblExt);
        }

        @Override
        protected boolean needSuicide() {
            //todo
            return false;
        }

        @Override
        protected Set<String> getMetadataDumpList(KylinConfig config) {
            final Set<String> dumpList = Sets.newHashSet();

            // dump project
            ProjectInstance instance = NProjectManager.getInstance(config).getProject(getProject());
            dumpList.add(instance.getResourcePath());

            // dump model if exist
            // dump raw table & table ext
            final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
            for (TableDesc table : tableMetadataManager.listAllTables()) {
                dumpList.add(table.getResourcePath());
                final TableExtDesc tableExtDesc = tableMetadataManager
                        .getTableExtIfExists(tableMetadataManager.getTableDesc(getTableIdentity()));
                if (tableExtDesc != null) {
                    dumpList.add(tableExtDesc.getResourcePath());
                }
            }

            return dumpList;
        }

        public static SamplingStep create(TableDesc tableDesc, String project, String submitter, int rows) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(submitter));
            Preconditions.checkArgument(tableDesc != null);
            Preconditions.checkArgument(StringUtils.isNotEmpty(project));

            SamplingStep job = new SamplingStep();
            job.setJobId(UUID.randomUUID().toString());
            job.setSubmitter(submitter);
            job.setProject(project);
            job.setProjectParam(project);
            job.setSamplingRows(rows);
            job.setTableIdentity(tableDesc.getIdentity());
            job.setDistMetaUrl(KylinConfig.getInstanceFromEnv().getJobTmpMetaStoreUrl(project, job.getId()).toString());
            job.setName(JobTypeEnum.TABLE_SAMPLING.toString());
            job.setJobType(JobTypeEnum.TABLE_SAMPLING);
            job.setSparkSubmitClassName(TableAnalyzerJob.class.getName());

            return job;
        }

    }

}
