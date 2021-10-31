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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_SEGMENT_CLEAN;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHousePartitionClean extends AbstractClickHouseClean {
    private String database;
    private String table;
    private Map<String, SegmentRange<Long>> segmentRangeMap;

    public ClickHousePartitionClean() {
        setName(STEP_SECOND_STORAGE_SEGMENT_CLEAN);
    }

    public ClickHousePartitionClean(Object notSetId) {
        super(notSetId);
    }

    public ClickHousePartitionClean setSegmentRangeMap(Map<String, SegmentRange<Long>> segmentRangeMap) {
        this.segmentRangeMap = segmentRangeMap;
        return this;
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        val segments = getTargetSegments();
        val dataflowManager = NDataflowManager.getInstance(config, getProject());
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());
        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());
        val dataflow = dataflowManager.getDataflow(getParam(NBatchConstants.P_DATAFLOW_ID));
        val tableFlow = Objects.requireNonNull(tableFlowManager.get().get(dataflow.getId()).orElse(null));
        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));
        segments.forEach(segment -> {
            nodeGroupManager.get().listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                    .forEach(node -> {
                        if (!tableFlow.getTableDataList().isEmpty()) {
                            database = NameUtil.getDatabase(dataflow);
                            table = NameUtil.getTable(dataflow, tableFlow.getTableDataList().get(0).getLayoutID());
                            ShardCleaner shardCleaner = segmentRangeMap.get(segment).isInfinite()
                                    ? new ShardCleaner(node, database, table, null, true)
                                    : new ShardCleaner(node, database, table,
                                    SecondStorageDateUtils.splitByDay(segmentRangeMap.get(segment)));
                            shardCleaners.add(shardCleaner);
                        }
                    });
        });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                shardCleaner.cleanPartitions();
            } catch (SQLException e) {
                log.error("node {} clean partitions {} in {}.{} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getPartitions(), shardCleaner.getDatabase(), shardCleaner.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }
}
