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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_MODEL_CLEAN;

import java.sql.SQLException;
import java.util.Objects;

import io.kyligence.kap.secondstorage.metadata.TableData;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseTableClean extends AbstractClickHouseClean {

    public ClickHouseTableClean() {
        setName(STEP_SECOND_STORAGE_MODEL_CLEAN);
    }

    public ClickHouseTableClean(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        val dataflowManager = NDataflowManager.getInstance(config, getProject());
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());
        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());
        val dataflow = dataflowManager.getDataflow(getParam(NBatchConstants.P_DATAFLOW_ID));

        val tableFlow = Objects.requireNonNull(tableFlowManager.get().get(getParam(NBatchConstants.P_DATAFLOW_ID)).orElse(null));

        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));
        nodeGroupManager.get().listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                .forEach(node -> {
                    for (TableData tableData : tableFlow.getTableDataList()) {
                        ShardCleaner shardCleaner = new ShardCleaner(node, NameUtil.getDatabase(dataflow),
                                NameUtil.getTable(dataflow, tableData.getLayoutID()));
                        shardCleaners.add(shardCleaner);
                    }
                });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                shardCleaner.cleanTable();
            } catch (SQLException e) {
                log.error("node {} clean table {}.{} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getDatabase(), shardCleaner.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }
}
