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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnTable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.stats.utils.HiveTableRefChecker;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NTableSamplingJob extends DefaultChainedExecutableOnTable {
    public NTableSamplingJob() {
        super();
    }

    public NTableSamplingJob(Object notSetId) {
        super(notSetId);
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, getProject()).getTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        Set<String> dumpList = Sets.newHashSet(tableDesc.getResourcePath(), projectInstance.getResourcePath());
        final TableExtDesc tableExtDesc = NTableMetadataManager.getInstance(config, getProject())
                .getTableExtIfExists(tableDesc);
        if (tableExtDesc != null) {
            dumpList.add(tableExtDesc.getResourcePath());
        }
        return dumpList;
    }

    public static NTableSamplingJob create(TableDesc tableDesc, String project, String submitter, int rows) {
        return create(tableDesc, project, submitter, rows, ExecutablePO.DEFAULT_PRIORITY, null, null);
    }

    public static NTableSamplingJob create(TableDesc tableDesc, String project, String submitter, int rows,
            int priority, String yarnQueue, Object tag) {
        Preconditions.checkArgument(tableDesc != null, //
                "Create table sampling job failed for table not exist!");

        log.info("start creating a table sampling job on table {}", tableDesc.getIdentity());
        NTableSamplingJob job = new NTableSamplingJob();
        job.setId(RandomUtil.randomUUIDStr());
        job.setName(JobTypeEnum.TABLE_SAMPLING.toString());
        job.setProject(project);
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setSubmitter(submitter);
        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, job.getId());
        job.setParam(NBatchConstants.P_TABLE_NAME, tableDesc.getIdentity());
        job.setParam(NBatchConstants.P_SAMPLING_ROWS, String.valueOf(rows));
        job.setPriority(priority);
        job.setSparkYarnQueueIfEnabled(project, yarnQueue);
        job.setTag(tag);

        KylinConfig globalConfig = KylinConfig.getInstanceFromEnv();
        KylinConfig config = NProjectManager.getInstance(globalConfig).getProject(project).getConfig();
        JobStepType.RESOURCE_DETECT.createStep(job, config);
        JobStepType.SAMPLING.createStep(job, config);
        if (HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(tableDesc.isTransactional(),
                tableDesc.isRangePartition(), config.isReadTransactionalTableEnabled())) {
            JobStepType.CLEAN_UP_TRANSACTIONAL_TABLE.createStep(job, config);
        }
        log.info("sampling job create success on table {}", tableDesc.getIdentity());
        return job;
    }

    @Override
    public boolean checkSuicide() {
        return null == NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getTableDesc(getTableIdentity());
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    SamplingStep getSamplingStep() {
        return getTask(SamplingStep.class);
    }

    public static class SamplingStep extends NSparkExecutable {

        // called by reflection
        public SamplingStep() {
        }

        public SamplingStep(Object notSetId) {
            super(notSetId);
        }

        SamplingStep(String sparkSubmitClassName) {
            this.setSparkSubmitClassName(sparkSubmitClassName);
            this.setName(ExecutableConstants.STEP_NAME_TABLE_SAMPLING);
        }

        private String getTableIdentity() {
            return getParam(NBatchConstants.P_TABLE_NAME);
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            ExecuteResult result = super.doWork(context);
            if (!result.succeed()) {
                return result;
            }
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                if (checkSuicide()) {
                    log.info(
                            "This Table Sampling job seems meaningless now, quit before mergeRemoteMetaAfterSampling()");
                    return null;
                }
                mergeRemoteMetaAfterSampling();
                return null;
            }, getProject());
            return result;
        }

        private void mergeRemoteMetaAfterSampling() {
            try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), this)) {
                val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), getProject());
                val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                localTblMgr.mergeAndUpdateTableExt(localTblMgr.getOrCreateTableExt(getTableIdentity()),
                        remoteTblMgr.getOrCreateTableExt(getTableIdentity()));

                // use create time of sampling job to update the create time of TableExtDesc
                final TableDesc tableDesc = localTblMgr.getTableDesc(getTableIdentity());
                final TableExtDesc tableExt = localTblMgr.getTableExtIfExists(tableDesc);
                TableExtDesc copyForWrite = localTblMgr.copyForWrite(tableExt);
                copyForWrite.setCreateTime(this.getCreateTime());
                localTblMgr.saveTableExt(copyForWrite);
            }
        }

        @Override
        protected Set<String> getMetadataDumpList(KylinConfig config) {

            final Set<String> dumpList = Sets.newHashSet();
            // dump project
            ProjectInstance instance = NProjectManager.getInstance(config).getProject(getProject());
            dumpList.add(instance.getResourcePath());

            // dump table & table ext
            final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
            final TableExtDesc tableExtDesc = tableMetadataManager
                    .getTableExtIfExists(tableMetadataManager.getTableDesc(getTableIdentity()));
            if (tableExtDesc != null) {
                dumpList.add(tableExtDesc.getResourcePath());
            }
            final TableDesc table = tableMetadataManager.getTableDesc(getTableIdentity());
            if (table != null) {
                dumpList.add(table.getResourcePath());
            }

            return dumpList;
        }
    }

}
