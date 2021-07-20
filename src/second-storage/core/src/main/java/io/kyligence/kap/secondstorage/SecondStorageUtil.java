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

package io.kyligence.kap.secondstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import static org.apache.kylin.common.exception.ServerErrorCode.BASE_TABLE_INDEX_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.PARTITION_COLUMN_NOT_AVAILABLE;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SecondStorageUtil {
    public static final Set<ExecutableState> RUNNING_STATE = Sets.newHashSet(
            Arrays.asList(ExecutableState.RUNNING, ExecutableState.READY, ExecutableState.PAUSED));
    public static final Set<JobTypeEnum> RELATED_JOBS = Sets.newHashSet(
            Arrays.asList(JobTypeEnum.INDEX_BUILD, JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INC_BUILD,
                    JobTypeEnum.INDEX_MERGE, JobTypeEnum.EXPORT_TO_SECOND_STORAGE));

    private SecondStorageUtil() {
    }

    public static void initModelMetaData(String project, String model) {
        checkEnableModel(project, model);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TablePlan>> tablePlanManager = tablePlanManager(config, project);
            Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(config, project);
            Preconditions.checkState(tableFlowManager.isPresent() && tablePlanManager.isPresent());
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
            TablePlan tablePlan = tablePlanManager.get().makeSureRootEntity(model);
            tableFlowManager.get().makeSureRootEntity(model);
            Map<Long, List<LayoutEntity>> layouts = indexPlanManager.getIndexPlan(model)
                    .getAllLayoutsMap().values().stream()
                    .filter(SecondStorageUtil::isBaseTableIndex)
                    .collect(Collectors.groupingBy(LayoutEntity::getIndexId));
            for (Map.Entry<Long, List<LayoutEntity>> entry : layouts.entrySet()) {
                LayoutEntity layoutEntity =
                        entry.getValue().stream().filter(SecondStorageUtil::isBaseTableIndex).findFirst().orElse(null);
                Preconditions.checkNotNull(layoutEntity);
                tablePlan = tablePlan.createTableEntityIfNotExists(layoutEntity, true);
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    private static void checkEnableModel(String project, String model) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        final IndexPlan indexPlan = indexPlanManager.getIndexPlan(model);
        if (!indexPlan.containBaseTableLayout()) {
            throw new KylinException(BASE_TABLE_INDEX_NOT_AVAILABLE, MsgPicker.getMsg().getBASE_TABLE_INDEX_NOT_AVAILABLE());
        }
        if (indexPlan.getModel().isIncrementBuildOnExpertMode()) {
            boolean containPartitionCol = indexPlan.getBaseTableLayout().getColumns().stream().anyMatch(col -> {
                return col.getTableDotName().equals(indexPlan.getModel().getPartitionDesc().getPartitionDateColumn());
            });
           if (!containPartitionCol) {
               throw new KylinException(PARTITION_COLUMN_NOT_AVAILABLE, MsgPicker.getMsg().getPARTITION_COLUMN_NOT_AVAILABLE());
           }
        }
    }

    public static boolean isBaseTableIndex(LayoutEntity index) {
        return IndexEntity.isTableIndex(index.getId()) && index.isBaseIndex();
    }

    public static Optional<LayoutEntity> getBaseIndex(NDataflow df) {
        return df.getIndexPlan().getAllLayouts().stream().filter(SecondStorageUtil::isBaseTableIndex).findFirst();
    }

    public static List<AbstractExecutable> findSecondStorageRelatedJobByProject(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        return executableManager.getJobs().stream().map(executableManager::getJob)
                .filter(job -> RELATED_JOBS.contains(job.getJobType()))
                .collect(Collectors.toList());
    }

    public static void validateDisableModel(String project, String modelId) {
        List<AbstractExecutable> jobs = SecondStorageUtil.findSecondStorageRelatedJobByProject(project);
        if (jobs.stream().filter(job -> RUNNING_STATE.contains(job.getStatus()))
                .anyMatch(job -> job.getTargetSubject().equals(modelId))) {
            String name = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDesc(modelId).getAlias();
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_JOB_EXISTS(), name));
        }
    }

    public static boolean isGlobalEnable() {
        return SecondStorage.enabled();
    }

    public static boolean isProjectEnable(String project) {
        if (isGlobalEnable()) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<NodeGroup>> nodeGroupManager = nodeGroupManager(config, project);
            return nodeGroupManager.isPresent() && !nodeGroupManager.get().listAll().isEmpty();
        }
        return false;
    }

    public static List<SecondStorageNode> listProjectNodes(String project) {
        if (!isProjectEnable(project)) {
            return Collections.emptyList();
        }
        Optional<Manager<NodeGroup>> groupManager = nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkNotNull(groupManager);
        return groupManager.map(nodeGroupNManager -> nodeGroupNManager.listAll().stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).distinct()
                .map(name -> {
                    Node node = SecondStorageNodeHelper.getNode(name);
                    return new SecondStorageNode().setIp(node.getIp()).setName(node.getName()).setPort(node.getPort());
                }).collect(Collectors.toList())).orElse(Collections.emptyList());
    }

    public static boolean isModelEnable(String project, String model) {
        if (isProjectEnable(project)) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(config, project);
            return tableFlowManager.isPresent() && tableFlowManager.get().get(model).isPresent();
        }
        return false;
    }

    public static List<SecondStorageInfo> setSecondStorageSizeInfo(List<NDataModel> models) {
        if (models == null || models.isEmpty()) {
            return Collections.emptyList();
        }
        Optional<Manager<TableFlow>> tableFlowManager =
                SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), models.get(0).getProject());
        Preconditions.checkState(tableFlowManager.isPresent());
        return setSecondStorageSizeInfo(models, tableFlowManager.get());
    }

    protected static List<SecondStorageInfo>
    setSecondStorageSizeInfo(List<NDataModel> models, Manager<TableFlow> tableFlowManager) {
        return models.stream().map(model -> {
            SecondStorageInfo secondStorageInfo = new SecondStorageInfo();
            secondStorageInfo.setSecondStorageEnabled(isModelEnable(model.getProject(), model.getId()));
            TableFlow tableFlow = tableFlowManager.get(model.getId()).orElse(null);
            if (isTableFlowEmpty(tableFlow)) {
                secondStorageInfo.setSecondStorageNodes(Collections.emptyList());
                secondStorageInfo.setSecondStorageSize(0);
            } else {
                TablePartition tablePartition = tableFlow.getTableDataList().get(0).getPartitions().get(0);
                secondStorageInfo.setSecondStorageNodes(tablePartition.getShardNodes().stream()
                        .map(SecondStorageUtil::transformNode).collect(Collectors.toList()));
                List<TablePartition> partitions = tableFlow.getTableDataList().stream()
                        .flatMap(tableData -> tableData.getPartitions().stream())
                        .collect(Collectors.toList());
                Long bytes = partitions.stream().map(partition -> partition.getSizeInNode().values()
                        .stream().reduce(Long::sum).orElse(0L)).reduce(Long::sum).orElse(0L);
                secondStorageInfo.setSecondStorageSize(bytes);
            }
            return secondStorageInfo;
        }).collect(Collectors.toList());
    }

    public static SecondStorageNode transformNode(String name) {
        Node node = SecondStorageNodeHelper.getNode(name);
        return new SecondStorageNode()
                .setIp(node.getIp())
                .setName(node.getName())
                .setPort(node.getPort());
    }

    public static boolean isTableFlowEmpty(TableFlow tableFlow) {
        return tableFlow == null
                || tableFlow.getTableDataList() == null
                || tableFlow.getTableDataList().isEmpty()
                || tableFlow.getTableDataList().get(0).getPartitions() == null
                || tableFlow.getTableDataList().get(0).getPartitions().isEmpty();
    }

    public static void disableProject(String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<NodeGroup>> nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, project);
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            Optional<Manager<TablePlan>> tablePlanManager = SecondStorageUtil.tablePlanManager(config, project);
            nodeGroupManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            tableFlowManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            tablePlanManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void disableModel(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            Optional<Manager<TablePlan>> tablePlanManager = SecondStorageUtil.tablePlanManager(config, project);
            tablePlanManager.ifPresent(manager -> manager.listAll().stream().filter(tablePlan -> tablePlan.getId().equals(modelId))
                    .forEach(manager::delete));
            tableFlowManager.ifPresent(manager -> manager.listAll().stream().filter(tableFlow -> tableFlow.getId().equals(modelId))
                    .forEach(manager::delete));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void cleanSegments(String project, String model, Set<String> segments) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            tableFlowManager.ifPresent(manager -> manager.listAll().stream().filter(tableFlow -> tableFlow.getId().equals(model))
                    .forEach(tableFlow -> {
                        tableFlow.update(copy -> {
                            copy.getTableDataList().stream().filter(tableData ->
                                    tableData.getDatabase().equals(NameUtil.getDatabase(config, project))
                                            && tableData.getTable().startsWith(NameUtil.tablePrefix(model)))
                                    .forEach(tableData -> tableData.removePartitions(
                                            tablePartition -> segments.contains(tablePartition.getSegmentId())));
                        });
                    }));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static Optional<Manager<TableFlow>> tableFlowManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.tableFlowManager(config, project)) : Optional.empty();
    }

    public static Optional<Manager<TableFlow>> tableFlowManager(NDataflow dataflow) {
        return isGlobalEnable() ? tableFlowManager(dataflow.getConfig(), dataflow.getProject()) : Optional.empty();
    }

    public static Optional<Manager<TablePlan>> tablePlanManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.tablePlanManager(config, project)) : Optional.empty();
    }

    public static Optional<Manager<NodeGroup>> nodeGroupManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.nodeGroupManager(config, project)) : Optional.empty();
    }
}