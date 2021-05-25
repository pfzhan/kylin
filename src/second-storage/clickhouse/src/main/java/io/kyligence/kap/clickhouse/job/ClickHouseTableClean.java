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

package io.kyligence.kap.clickhouse.job;

import com.clearspring.analytics.util.Preconditions;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;

import java.sql.SQLException;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_MODEL_CLEAN;

@Slf4j
public class ClickHouseTableClean extends AbstractClickHouseClean {

    public ClickHouseTableClean() {
        setName(STEP_SECOND_STORAGE_MODEL_CLEAN);
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        val dataflowManager = NDataflowManager.getInstance(config, getProject());
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());
        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());
        val dataflow = dataflowManager.getDataflow(getParam(NBatchConstants.P_DATAFLOW_ID));
        val tableFlow = tableFlowManager.flatMap(manager -> manager.get(dataflow.getId()));
        setNodeCount(Math.toIntExact(nodeGroupManager.map(manager -> manager.listAll().stream()
                .mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum()).orElse(0L)));
        nodeGroupManager.get().listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).forEach(node -> {
            if (tableFlow.isPresent() && !tableFlow.get().getTableDataList().isEmpty()) {
                ShardClean shardClean = new ShardClean(node,
                        SecondStorageUtil.getDatabase(dataflow),
                        SecondStorageUtil.getTable(dataflow, tableFlow.get().getTableDataList().get(0).getLayoutID()));
                shardCleanList.add(shardClean);
            }
        });
    }

    @Override
    protected Runnable getTask(ShardClean shardClean) {
        return () -> {
            try {
                shardClean.cleanTable();
            } catch (SQLException e) {
                log.error("node {} clean table {}.{} failed", shardClean.getClickHouse().getShardName(),
                        shardClean.getDatabase(),
                        shardClean.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }

    @Override
    protected void updateMetaData() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = getConfig();
            val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());
            val tablePlanManager = SecondStorageUtil.tablePlanManager(config, getProject());
            Preconditions.checkState(tablePlanManager.isPresent() && tableFlowManager.isPresent());
            tablePlanManager.get().listAll().stream().filter(tablePlan -> tablePlan.getId().equals(getTargetModelId()))
                    .forEach(tablePlanManager.get()::delete);
            tableFlowManager.get().listAll().stream().filter(tableFlow -> tableFlow.getId().equals(getTargetModelId()))
                    .forEach(tableFlowManager.get()::delete);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }
}
