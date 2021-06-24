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

package io.kyligence.kap.secondstorage.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.NManager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SecondStorageConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_NODE_NOT_AVAILABLE;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.SecondStorageModelCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageProjectCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


public class SecondStorageService extends BasicService implements SecondStorageUpdater {

    private AclEvaluate aclEvaluate;

    @Autowired
    public SecondStorageService setAclEvaluate(final AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    public boolean isEnabled(String project, String modelId) {
        return SecondStorageUtil.isModelEnable(project, modelId);
    }

    public Optional<TableFlow> getTableFlow(String project, String modelId) {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(tableFlowManager.isPresent());
        return tableFlowManager.get().listAll().stream().filter(tableFlow -> tableFlow.getId().equals(modelId)).findFirst();
    }

    @Transaction(project = 0)
    public Optional<JobInfoResponse.JobInfo> changeModelSecondStorageState(String project, String modelId, boolean enabled) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv())
            aclEvaluate.checkProjectAdminPermission(project);
        JobInfoResponse.JobInfo jobInfo = null;
        if (enabled) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                enableModelSecondStorage(project, modelId);
                return null;
            }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        } else {
            val jobId = disableModelSecondStorage(project, modelId);
            jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.name(), jobId);
        }
        return Optional.ofNullable(jobInfo);
    }

    @Transaction(project = 0)
    public Optional<JobInfoResponse.JobInfo> changeProjectSecondStorageState(String project, List<String> nodes, boolean enable) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv())
            aclEvaluate.checkProjectAdminPermission(project);
        JobInfoResponse.JobInfo jobInfo = null;
        if (enable) {
            if (!listAvailableNodes().stream()
                    .map(NodeData::getName).collect(Collectors.toSet()).containsAll(nodes)) {
                throw new KylinException(SECOND_STORAGE_NODE_NOT_AVAILABLE, MsgPicker.getMsg().getSECOND_STORAGE_NODE_NOT_AVAILABLE());
            }
            if (!SecondStorageUtil.isProjectEnable(project)) {
                enableProjectSecondStorage(project, nodes);
            }
            addNodeToProject(project, nodes);
        } else {
            String jobId = disableProjectSecondStorage(project);
            jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.name(), jobId);
        }
        return Optional.ofNullable(jobInfo);
    }

    public void enableProjectSecondStorage(String project, List<String> nodes) {
        Preconditions.checkArgument(listAvailableNodes().stream()
                .map(NodeData::getName).collect(Collectors.toSet()).containsAll(nodes));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
            Preconditions.checkState(nodeGroupManager.isPresent());
            int replicaNum = SecondStorageConfig.getInstanceFromEnv().getReplicaNum();
            for (int i = 0; i < replicaNum; i++) {
                nodeGroupManager.get().makeSureRootEntity("");
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public void addNodeToProject(String project, List<String> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        int replicaNum = SecondStorageConfig.getInstanceFromEnv().getReplicaNum();
        Map<Integer, List<String>> replicaNodes = SecondStorageNodeHelper
                .separateReplicaGroup(replicaNum, nodes.toArray(new String[0]));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val internalManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
            Preconditions.checkState(internalManager.isPresent());
            val allGroups = internalManager.get().listAll();
            for (Integer idx : replicaNodes.keySet()) {
                allGroups.get(idx).update(copied -> {
                    val nodeBuffer = Lists.newArrayList(copied.getNodeNames());
                    nodeBuffer.addAll(replicaNodes.get(idx));
                    copied.setNodeNames(nodeBuffer);
                });
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public String disableProjectSecondStorage(String project) {
        // check related jobs
        val models = this.validateProjectDisable(project);
        if (!models.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_PROJECT_JOB_EXISTS,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_PROJECT_JOB_EXISTS(), project));
        }
        val jobId = triggerProjectClean(project);
        SecondStorageUtil.disableProject(project);
        return jobId;
    }

    private String triggerProjectClean(String project) {
        val jobHandler = new SecondStorageProjectCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.projectCleanParam(project, getUsername());
        return getJobManager(project).addJob(param, jobHandler);
    }

    private String triggerModelClean(String project, String model) {
        val jobHandler = new SecondStorageModelCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, model, getUsername());
        return getJobManager(project).addJob(param, jobHandler);
    }

    @Transaction(project = 0)
    public String triggerSegmentsClean(String project, String model, Set<String> segIds) {
        Preconditions.checkState(SecondStorageUtil.isModelEnable(project, model));
        SecondStorageUtil.cleanSegments(project, model, segIds);
        val jobHandler = new SecondStorageSegmentCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.segmentCleanParam(project, model, getUsername(), segIds);
        return getJobManager(project).addJob(param, jobHandler);
    }


    public List<NodeData> listAvailableNodes() {
        val config = KylinConfig.getInstanceFromEnv();
        val usedNodes = NProjectManager.getInstance(config).listAllProjects().stream().flatMap(projectInstance -> {
            NManager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
            return nodeGroupManager.listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream());
        }).collect(Collectors.toSet());
        List<String> allNodes = SecondStorageNodeHelper.getAllNames();
        return allNodes.stream()
                .filter(node -> !usedNodes.contains(node))
                .map(name -> new NodeData(SecondStorageNodeHelper.getNode(name)))
                .collect(Collectors.toList());
    }

    public void enableModelSecondStorage(String project, String modelId) {
        if (isEnabled(project, modelId)) {
            return;
        }
        val indexPlanManager = getIndexPlanManager(project);
        final IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        if (!indexPlan.containBaseTableLayout() && !indexPlan.getModel().getEffectiveDimensions().isEmpty()) {
            indexPlanManager.updateIndexPlan(modelId, copied -> {
                copied.createAndAddBaseIndex(Collections.singletonList(copied.createBaseTableIndex(copied.getModel())));
            });
        }
        SecondStorageUtil.initModelMetaData(project, modelId);
    }

    public String disableModelSecondStorage(String project, String modelId) {
        if (!isEnabled(project, modelId)) {
            return "";
        }
        SecondStorageUtil.validateDisableModel(project, modelId);
        val jobId = triggerModelClean(project, modelId);
        SecondStorageUtil.disableModel(project, modelId);
        return jobId;
    }

    public List<String> getAllSecondStorageModel(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        return modelManager.listAllModels().stream().filter(model -> SecondStorageUtil.isModelEnable(project, model.getId()))
                .map(NDataModel::getAlias).collect(Collectors.toList());
    }

    public List<String> validateProjectDisable(String project) {
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.getAllExecutables();
        val allJobs = executableManager.getJobs().stream()
                .map(executableManager::getJob)
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatus()))
                .filter(job -> SecondStorageUtil.RELATED_JOBS.contains(job.getJobType()))
                .collect(Collectors.toList());
        if (allJobs.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> models = new HashSet<>();
        allJobs.forEach(job -> {
            if (SecondStorageUtil.isModelEnable(job.getProject(), job.getTargetSubject())) {
                models.add(job.getTargetSubject());
            }
        });
        return Lists.newArrayList(models);
    }

    public void isProjectAdmin(String project) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            aclEvaluate.checkProjectAdminPermission(project);
        }
    }

    @Override
    @Transaction(project = 0)
    public void onUpdate(final String project, final String modelId) {
        if (!SecondStorageUtil.isModelEnable(project, modelId)) return;
        val tableFlowManager = SecondStorageUtil.tableFlowManager(getConfig(), project);
        val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        Preconditions.checkState(tablePlanManager.isPresent());
        Preconditions.checkState(tableFlowManager.isPresent());
        val tableFlow = tableFlowManager.get().get(modelId);
        Preconditions.checkState(tableFlow.isPresent());
        if (indexPlan.getBaseTableLayout() != null) {
            val dataflowManager = NDataflowManager.getInstance(getConfig(), project);
            if (!dataflowManager.getDataflow(modelId).getSegments().isEmpty()) {
                // when segment exits, trigger model clean job
                triggerModelClean(project, modelId);
            }
            tableFlowManager.get().get(modelId).map(tf -> {
                tf.update(TableFlow::cleanTableData);
                return tf;
            });
            tablePlanManager.get().get(modelId).map(tp -> {
                tp = tp.update(TablePlan::cleanTable);
                tp.createTableEntityIfNotExists(indexPlan.getBaseTableLayout(), true);
                return tp;
            });
        }
    }

    public void refreshConf() {
        aclEvaluate.checkIsGlobalAdmin();
        SecondStorage.init(true);
    }
}
