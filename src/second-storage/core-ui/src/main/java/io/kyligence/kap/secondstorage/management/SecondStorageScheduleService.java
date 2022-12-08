/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.secondstorage.management;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.scheduling.annotation.Scheduled;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageLockUtils;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.database.DatabaseOperator;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.factory.SecondStorageFactoryUtils;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SecondStorageScheduleService {
    private static final int JOB_ID_LENGTH = 36;

    @Scheduled(cron = "${kylin.second-storage.table-clean-cron:0 0 0 * * *}")
    public void secondStorageTempTableCleanTask() {
        cleanAllUsedNode();
    }

    @Scheduled(cron = "${kylin.second-storage.low-cardinality-cron:0 0 0 * * *}")
    public void secondStorageLowCardinality() throws Exception {
        log.info("Start to modify second storage low cardinality.");
        EpochManager epochManager = EpochManager.getInstance();
        if (!epochManager.checkEpochOwner(EpochManager.GLOBAL))
            return;

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.getSecondStorageUseLowCardinality())
            return;

        val projectManager = NProjectManager.getInstance(config);
        List<String> enabledProjects = projectManager.listAllProjects().stream()
                .filter(project -> SecondStorageUtil.isProjectEnable(project.getName())
                        && project.getConfig().getSecondStorageUseLowCardinality())
                .map(ProjectInstance::getName)
                .collect(Collectors.toList());

        for (String project : enabledProjects) {
            try {
                SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.ALL.name()));
            } catch (KylinException e) {
                log.error("There is second storage task on project {}.", project);
                continue;
            }
            log.info("Start to modify second storage low cardinality on project {}.", project);
            val dataModelManager = NDataModelManager.getInstance(config, project);
            QueryOperator queryOperator = SecondStorageFactoryUtils.createQueryMetricOperator(project);
            List<NDataModel> enabledModels = dataModelManager.listAllModels().stream()
                    .filter(model -> SecondStorageUtil.isModelEnable(project, model.getId())
                            && !SecondStorageLockUtils.containsKey(model.getId()))
                    .collect(Collectors.toList());
            for (NDataModel model : enabledModels) {
                val df = NDataflowManager.getInstance(config, project).getDataflow(model.getId());
                if (df.getConfig().getSecondStorageUseLowCardinality()) {
                    SegmentRange<Long> range = new SegmentRange.TimePartitionedSegmentRange(df.getSegments().getTSStart(), df.getSegments().getTSEnd());
                    SecondStorageLockUtils.acquireLock(model.getId(), range).lock();
                    try {
                        val database = NameUtil.getDatabase(df);
                        val destTableName = NameUtil.getTable(df, SecondStorageUtil.getBaseIndex(df).getId());
                        val tablePlanManager = SecondStorageUtil.tablePlanManager(config, project);
                        TablePlan tablePlan = tablePlanManager.get().get(model.getId()).get();
                        TableEntity tableEntity = tablePlan.getEntity(SecondStorageUtil.getBaseIndex(df).getId()).orElse(null);
                        queryOperator.modifyColumnByCardinality(database, destTableName, tableEntity.getSecondaryIndexColumns());
                    } catch (Exception exception) {
                        log.error("Failed to modify second storage low cardinality on model {}.", model.getId(), exception);
                    } finally {
                        SecondStorageLockUtils.unlock(model.getId(), range);
                    }
                }
            }
            log.info("Finish to modify second storage low cardinality on project {}.", project);
        }
    }

    private void cleanAllUsedNode() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        List<String> enabledProjects = projectManager.listAllProjects().stream().map(ProjectInstance::getName)
                .filter(SecondStorageUtil::isProjectEnable)
                .collect(Collectors.toList());
        Map<String, List<String>> projectNodeMap = Maps.newHashMap();
        enabledProjects.forEach(project -> {
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, project);
            // get all node in project
            nodeGroupManager.ifPresent(groupManager -> projectNodeMap.put(project, groupManager.listAll().stream()
                    .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).collect(Collectors.toList())));
        });
        projectNodeMap.forEach((project, nodes) -> {
            if (!nodes.isEmpty()) {
                log.info("start clean second storage temp table on project {}.", project);
                // clean single node
                for (final String node : nodes) {
                    log.info("start clean second storage temp table on project {} node {}.", project, node);
                    try {
                        cleanSingleNode(project, node);
                    } catch (IOException e) {
                        log.error("node {} connect failed", node, e);
                    }
                }
            }
        });
    }

    private void cleanSingleNode(String project, String node) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (DatabaseOperator operator = SecondStorageFactoryUtils.createDatabaseOperator(SecondStorageNodeHelper.resolve(node))) {
            String database = NameUtil.getDatabase(config, project);
            List<String> allDatabases = operator.listDatabases();
            if (!allDatabases.contains(database)) {
                return;
            }

            // get all temp table in database
            List<String> tempTables = operator.listTables(database).stream()
                    .filter(NameUtil::isTempTable).map(table -> table.replace("-", "_")).collect(Collectors.toList());

            val execManager = ExecutableManager.getInstance(config, project);
            val allJobs = execManager.getAllJobs();
            List<String> discardJobs = allJobs.stream()
                    .filter(job -> job.getOutput().getStatus().equals(ExecutableState.DISCARDED.name()))
                    .map(RootPersistentEntity::getId)
                    .map(jobId -> jobId.length() > JOB_ID_LENGTH ? jobId.substring(0, JOB_ID_LENGTH) : jobId)
                    .map(jobId -> jobId.replace("-", "_"))
                    .collect(Collectors.toList());

            List<String> allJobIds = allJobs.stream()
                    .map(RootPersistentEntity::getId)
                    .map(jobId -> jobId.length() > JOB_ID_LENGTH ? jobId.substring(0, JOB_ID_LENGTH) : jobId)
                    .map(jobId -> jobId.replace("-", "_"))
                    .collect(Collectors.toList());

            // temp table is start with job id
            List<String> discardTempTables = tempTables.stream()
                    .filter(table -> discardJobs.contains(table.substring(0, JOB_ID_LENGTH))).collect(Collectors.toList());

            // a temp table doesn't belong to any job
            List<String> orphanTempTables = tempTables.stream().filter(table -> !allJobIds.contains(table.substring(0, JOB_ID_LENGTH)))
                    .collect(Collectors.toList());
            log.info("check database {}, find discardTempTables: {}, orphanTempTables: {} ", database, discardTempTables, orphanTempTables);

            discardTempTables.forEach(table -> operator.dropTable(database, table));
            orphanTempTables.forEach(table -> operator.dropTable(database, table));
        }
    }
}
