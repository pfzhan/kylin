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
package io.kyligence.kap.secondstorage.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;
import scala.collection.JavaConverters;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@DataDefinition
public class TableData implements Serializable, WithLayout {


    public static final class Builder {

        private LayoutEntity layoutEntity;
        private PartitionType partitionType;
        private String database;
        private String table;

        public Builder setLayoutEntity(LayoutEntity layoutEntity) {
            this.layoutEntity = layoutEntity;
            return this;
        }

        public Builder setPartitionType(PartitionType partitionType) {
            this.partitionType = partitionType;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public TableData build() {
            TableData tableData = new TableData();
            tableData.layoutID = layoutEntity.getId();
            tableData.partitionType = this.partitionType;
            tableData.table = this.table;
            tableData.database = this.database;
            return tableData;
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    @JsonBackReference
    private TableFlow tableFlow;

    @JsonProperty("layout_id")
    private long layoutID;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("partition_type")
    private PartitionType partitionType;

    @JsonProperty("partitions")
    private final List<TablePartition> partitions = Lists.newArrayList();

    private Set<String> allSegmentIds;

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    @Override
    public long getLayoutID() {
        return layoutID;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    protected void checkIsNotCachedAndShared() {
        if (tableFlow != null)
            tableFlow.checkIsNotCachedAndShared();
    }

    // create
   public void addPartition(TablePartition partition) {
       Preconditions.checkArgument(partition != null);
       checkIsNotCachedAndShared();
       partitions.removeIf(p ->
               isSameGroup(p.getShardNodes(), partition.getShardNodes()) && p.getSegmentId().equals(partition.getSegmentId())
       );
       partitions.add(partition);
   }

    // read
    public List<TablePartition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    // update & delete
    public void removePartitions(Predicate<? super TablePartition> filter) {
        checkIsNotCachedAndShared();
        partitions.removeIf(filter);
    }

    public void mergePartitions(Set<String> oldSegIds, String mergeSegId) {
        checkIsNotCachedAndShared();
        List<Set<String>> nodeGroups = new ArrayList<>();
        Map<String, Long> sizeInNode = new HashMap<>();
        Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>();
        getPartitions().stream().filter(partition -> oldSegIds.contains(partition.getSegmentId()))
                .forEach(partition -> {
                    partition.getSizeInNode().forEach((key, value) ->
                        sizeInNode.put(key, sizeInNode.getOrDefault(key, 0L) + value));

                    partition.getNodeFileMap().forEach((key, value) ->
                        nodeFileMap.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));

                    for (Set<String> nodeGroup : nodeGroups) {
                        if (isSameGroup(nodeGroup, partition.getShardNodes())) {
                            nodeGroup.addAll(partition.getShardNodes());
                            return;
                        }
                    }
                    nodeGroups.add(new HashSet<>(partition.getShardNodes()));
                });
        List<TablePartition> mergedPartitions = nodeGroups.stream().map(nodeGroup ->
                TablePartition.builder()
                    .setSizeInNode(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> sizeInNode.getOrDefault(node, 0L))))
                    .setSegmentId(mergeSegId)
                    .setShardNodes(Lists.newArrayList(nodeGroup))
                    .setId(RandomUtil.randomUUIDStr())
                    .setNodeFileMap(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> nodeFileMap.getOrDefault(node, Collections.emptyList()))
                    )).build()
        ).collect(Collectors.toList());
        removePartitions(tablePartition -> oldSegIds.contains(tablePartition.getSegmentId()));
        this.partitions.addAll(mergedPartitions);
    }

    // utility
    private boolean isSameGroup(Collection<String> a, Collection<String> b) {
        Set<String> aSet = Sets.newHashSet(a);
        Set<String> bSet = Sets.newHashSet(b);
        return aSet.containsAll(bSet) || bSet.containsAll(aSet);
    }

    public String getShardJDBCURLs() {
        if (partitions.isEmpty()) {
            return null;
        }
        TablePartition nextPartition = SecondStorageQueryRouteUtil.getNextPartition(partitions);
        List<String> nodes = SecondStorageNodeHelper.resolveToJDBC(nextPartition.getShardNodes());
        return ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(nodes));
    }

    public String getSchemaURL() {
        if (partitions.isEmpty()) {
            return null;
        }
        List<String> nodes = SecondStorageNodeHelper.resolveToJDBC(SecondStorageQueryRouteUtil.getCurrentPartition(partitions).getShardNodes());
        return nodes.get(0);
    }

    public boolean containSegments(Set<String> segmentIds) {
        if (allSegmentIds == null) {
            allSegmentIds = partitions.stream().map(TablePartition::getSegmentId).collect(Collectors.toSet());
        }
        return allSegmentIds.containsAll(segmentIds);
    }
    // replace?
}
