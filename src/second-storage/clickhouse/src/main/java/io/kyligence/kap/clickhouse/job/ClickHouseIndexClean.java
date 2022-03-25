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
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_INDEX_CLEAN;

@Slf4j
public class ClickHouseIndexClean extends AbstractClickHouseClean {
    private Set<Long> needDeleteLayoutIds;

    private String dateFormat;

    private Map<String, SegmentRange<Long>> segmentRangeMap;

    // can't delete because reflect
    public ClickHouseIndexClean() {
        setName(STEP_SECOND_STORAGE_INDEX_CLEAN);
    }

    // can't delete because reflect
    public ClickHouseIndexClean(Object notSetId) {
        super(notSetId);
    }

    public void setNeedDeleteLayoutIds(Set<Long> needDeleteLayoutIds) {
        this.needDeleteLayoutIds = needDeleteLayoutIds;
    }

    public Set<Long> getNeedDeleteLayoutIds() {
        return this.needDeleteLayoutIds;
    }

    public ClickHouseIndexClean setSegmentRangeMap(Map<String, SegmentRange<Long>> segmentRangeMap) {
        this.segmentRangeMap = segmentRangeMap;
        return this;
    }

    public void setDateFormat(final String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        String modelId = getParam(NBatchConstants.P_DATAFLOW_ID);
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());

        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());

        val tableFlow = Objects.requireNonNull(tableFlowManager.get().get(modelId).orElse(null));

        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));


        List<String> nodes = nodeGroupManager.get().listAll()
                .stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                .collect(Collectors.toList());


        getNeedDeleteLayoutIds().forEach(layoutId -> {
            // table_data not contains layout means deleted. Delete table instead partition
            if (segmentRangeMap.isEmpty() || !tableFlow.getEntity(layoutId).isPresent()) {
                shardCleaners.addAll(cleanTable(nodes, layoutId));
            } else {
                segmentRangeMap.keySet().forEach(segmentId -> shardCleaners.addAll(cleanPartition(nodes, layoutId, segmentId)));
            }
        });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                if (shardCleaner.getPartitions() == null) {
                    shardCleaner.cleanTable();
                } else {
                    shardCleaner.cleanPartitions();
                }
            } catch (SQLException e) {
                log.error("node {} clean index {}.{} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getDatabase(), shardCleaner.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }

    private List<ShardCleaner> cleanTable(List<String> nodes, long layoutId) {
        return nodes.stream().map(node ->
                new ShardCleaner(node, NameUtil.getDatabase(getConfig(), project),
                        NameUtil.getTable(getParam(NBatchConstants.P_DATAFLOW_ID), layoutId))
        ).collect(Collectors.toList());
    }

    private List<ShardCleaner> cleanPartition(List<String> nodes, long layoutId, String segmentId) {
        return nodes.stream().map(node ->
                new ShardCleaner(node, NameUtil.getDatabase(getConfig(), project), NameUtil.getTable(getParam(NBatchConstants.P_DATAFLOW_ID), layoutId),
                        SecondStorageDateUtils.splitByDay(segmentRangeMap.get(segmentId)), getDateFormat())
        ).collect(Collectors.toList());
    }
}
