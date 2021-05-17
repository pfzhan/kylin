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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@DataDefinition
public class TableData implements Serializable, IKeep, WithLayout {

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
       partitions.removeIf(p -> {
           return isSameGroup(p.getShardNodes(), partition.getShardNodes()) && p.getSegmentId().equals(partition.getSegmentId());
       });
       partitions.add(partition);
   }

    // read
    public List<TablePartition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    // delete
    public void removePartitions(Predicate<? super TablePartition> filter) {
        partitions.removeIf(filter);
    }

    public void mergePartitions(Set<String> oldSegIds, String mergeSegId) {
        List<Set<String>> nodeGroups = new ArrayList<>();
        Map<String, Long> sizeInNode = new HashMap<>();
        Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>();
        getPartitions().stream().filter(partition -> oldSegIds.contains(partition.getSegmentId()))
                .forEach(partition -> {
                    partition.getSizeInNode().forEach((key, value) -> {
                        sizeInNode.put(key, sizeInNode.getOrDefault(key, 0L) + value);
                    });
                    partition.getNodeFileMap().forEach((key, value) -> {
                        nodeFileMap.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value);
                    });
                    for (Set<String> nodeGroup : nodeGroups) {
                        if (isSameGroup(nodeGroup, partition.getShardNodes())) {
                            nodeGroup.addAll(partition.getShardNodes());
                            return;
                        }
                    }
                    nodeGroups.add(new HashSet<>(partition.getShardNodes()));
                });
        List<TablePartition> mergedPartitions = nodeGroups.stream().map(nodeGroup -> {
            return TablePartition.builder()
                    .setSizeInNode(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> sizeInNode.getOrDefault(node, 0L))))
                    .setSegmentId(mergeSegId)
                    .setShardNodes(Lists.newArrayList(nodeGroup))
                    .setId(UUID.randomUUID().toString())
                    .setNodeFileMap(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> nodeFileMap.getOrDefault(node, Collections.emptyList()))
                    )).build();
        }).collect(Collectors.toList());
        removePartitions(tablePartition -> oldSegIds.contains(tablePartition.getSegmentId()));
        this.partitions.addAll(mergedPartitions);
    }

    private boolean isSameGroup(Collection<String> a, Collection<String> b) {
        Set<String> aSet = Sets.newHashSet(a);
        Set<String> bSet = Sets.newHashSet(b);
        return aSet.containsAll(bSet) || bSet.containsAll(aSet);
    }

    // replace?
}
