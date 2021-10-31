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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.base.Preconditions;

import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;

public class LoadInfo {
    final NDataModel model;
    final NDataSegment segment;
    final String segmentId; // it is required for updating meta after load
    private String oldSegmentId;
    final String[] nodeNames;
    final LayoutEntity layout;
    final List<List<SegmentFileStatus>> shardFiles;

    private String targetDatabase;
    private String targetTable;

    @SuppressWarnings("unchecked")
    private static <T> List<T> newFixedSizeList(int size) {
        return (List<T>) Arrays.asList(new Object[size]);
    }

    private LoadInfo(NDataModel model, NDataSegment segment, LayoutEntity layout, String[] nodeNames) {
        this(model, segment, null, layout, nodeNames);
    }

    private LoadInfo(NDataModel model, NDataSegment segment, String oldSegmentId, LayoutEntity layout,
            String[] nodeNames) {
        this.model = model;
        this.segment = segment;
        final int shardNumber = nodeNames.length;
        this.nodeNames = nodeNames;
        this.segmentId = segment.getId();
        this.layout = layout;
        this.shardFiles = newFixedSizeList(shardNumber);
        this.oldSegmentId = oldSegmentId;
        for (int i = 0; i < shardNumber; ++i) {
            this.shardFiles.set(i, new ArrayList<>(100));
        }
    }

    /**
     * ClickHouse doesn't support the separation of storage and compute, so it's hard to scale horizontally.
     * It results in the long term use of a fixed number of shards. We have two cases:
     * <ul>
     *    <li>Full Load</li>
     *    <li>Incremental Load</li>
     * </ul>
     * The problem here is how to distribute files across multiple shards evenly in incremental load? Consider the
     * case where table index building always generates 10 parquet files every day, and unfortunately, we only have 3
     * shards. If we always distribute files from index 0, then shard 0 will have 10 more files than the other two after
     * ten days. i.e.
     * <ul>
     *     <li>shard 0: 40
     *     <li>shard 1: 30
     *     <li>shard 2: 40
     * </ul>
     *
     * TODO: Incremental Load
     * TODO: Use a simple way to avoid this issue -- randomly choose the start shard each time we distribute loads.
     * TODO: fault-tolerant for randomly choosing the start shard?
     *
     */

    public static LoadInfo distribute(String[] nodeNames, NDataModel model, NDataSegment segment, FileProvider provider,
            LayoutEntity layout) {
        int shardNum = nodeNames.length;
        final LoadInfo info = new LoadInfo(model, segment, layout, nodeNames);
        val it = provider.getAllFilePaths().iterator();
        int index = 0;
        while (it.hasNext()) {
            FileStatus fileStatus = it.next();
            info.addShardFile(index, fileStatus.getPath(), fileStatus.getLen());
            index = ++index % shardNum;
        }
        return info;
    }

    public LoadInfo setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
        return this;
    }

    public LoadInfo setTargetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public LoadInfo setOldSegmentId(String oldSegmentId) {
        this.oldSegmentId = oldSegmentId;
        return this;
    }

    private void addShardFile(int shardIndex, String filePath, long fileLen) {
        Preconditions.checkArgument(shardIndex < shardFiles.size());
        shardFiles.get(shardIndex).add(SegmentFileStatus.builder().setLen(fileLen).setPath(filePath).build());
    }

    public LayoutEntity getLayout() {
        return layout;
    }

    public List<List<String>> getShardFiles() {
        return shardFiles.stream()
                .map(files -> files.stream().map(SegmentFileStatus::getPath).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    public String getSegmentId() {
        return segmentId;
    }

    public String[] getNodeNames() {
        return nodeNames;
    }

    // meta update
    public TablePartition createMetaInfo() {
        Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>();
        ListIterator<String> it = Arrays.asList(nodeNames).listIterator();
        while (it.hasNext()) {
            int idx = it.nextIndex();
            nodeFileMap.put(it.next(), shardFiles.get(idx));
        }
        val metric = new ClickHouseTableStorageMetric(Arrays.asList(this.nodeNames));
        metric.collect();
        Map<String, Long> sizeInNode;
        Preconditions.checkNotNull(targetDatabase);
        Preconditions.checkNotNull(targetTable);
        if (segment.getSegRange().isInfinite()) {
            sizeInNode = metric.getByPartitions(targetDatabase, targetTable, Collections.singletonList("tuple()"));
        } else {
            SimpleDateFormat partitionFormat = new SimpleDateFormat(model.getPartitionDesc().getPartitionDateFormat(), Locale.getDefault(Locale.Category.FORMAT));
            sizeInNode = metric.getByPartitions(targetDatabase, targetTable,
                    SecondStorageDateUtils.splitByDay((SegmentRange<Long>) segment.getSegRange()).stream()
                            .map(partitionFormat::format).collect(Collectors.toList()));
        }
        return TablePartition.builder().setSegmentId(segmentId).setShardNodes(Arrays.asList(nodeNames))
                .setId(RandomUtil.randomUUIDStr()).setNodeFileMap(nodeFileMap).setSizeInNode(sizeInNode).build();
    }

    public void upsertTableData(TableFlow copied, String database, String table, PartitionType partitionType) {
        copied.upsertTableData(layout, tableData -> {
            Preconditions.checkArgument(tableData.getPartitionType() == partitionType);
            if (oldSegmentId != null) {
                tableData.removePartitions(tablePartition -> tablePartition.getSegmentId().equals(oldSegmentId));
            }
            tableData.addPartition(createMetaInfo());
        }, () -> TableData.builder().setPartitionType(partitionType).setLayoutEntity(getLayout()).setDatabase(database)
                .setTable(table).build());
    }
}