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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Table Metadata from Source. All name should be uppercase.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class TableDesc extends RootPersistentEntity implements Serializable, ISourceAware {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TableDesc.class);

    private static final String TABLE_TYPE_VIEW = "VIEW";
    private static final String materializedTableNamePrefix = "kylin_intermediate_";

    // returns <table, project>
    public static Pair<String, String> parseResourcePath(String path) {
        if (path.endsWith(".json"))
            path = path.substring(0, path.length() - ".json".length());

        int cut = path.lastIndexOf("/");
        if (cut >= 0)
            path = path.substring(cut + 1);

        String table, prj;
        int dash = path.indexOf("--");
        if (dash >= 0) {
            table = path.substring(0, dash);
            prj = path.substring(dash + 2);
        } else {
            table = path;
            prj = null;
        }
        return Pair.newPair(table, prj);
    }

    // ============================================================================

    private String name;

    @Getter
    @Setter
    @JsonProperty("columns")
    private ColumnDesc[] columns;

    @JsonProperty("source_type")
    private int sourceType = ISourceAware.ID_HIVE;

    @JsonProperty("table_type")
    private String tableType;

    //Sticky table
    @Getter
    @Setter
    @JsonProperty("top")
    private boolean isTop;
    @JsonProperty("data_gen")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataGen;

    @Getter
    @Setter
    @JsonProperty("increment_loading")
    private boolean incrementLoading;

    @Getter
    @Setter
    @JsonProperty("last_snapshot_path")
    private String lastSnapshotPath;

    @Getter
    @Setter
    @JsonProperty("last_snapshot_size")
    private long lastSnapshotSize;

    @Getter
    @Setter
    @JsonProperty("snapshot_last_modified")
    private long snapshotLastModified;

    @Getter
    @Setter
    @JsonProperty("query_hit_count")
    private int snapshotHitCount = 0;

    //first level partition col for this table
    @Setter
    @Getter
    @JsonProperty("partition_column")
    private String partitionColumn;

    @Getter
    @Setter
    @JsonProperty("snapshot_partitions")
    private Map<String, Long> snapshotPartitions = Maps.newHashMap();

    //partition col of current built snapshot
    @Getter
    @Setter
    @JsonProperty("snapshot_partition_col")
    private String snapshotPartitionCol;

    // user select partition for this snapshot table
    @Setter
    @Getter
    @JsonProperty("selected_snapshot_partition_col")
    private String selectedSnapshotPartitionCol;

    @Setter
    @Getter
    @JsonProperty("temp_snapshot_path")
    private String tempSnapshotPath;

    @Setter
    @Getter
    @JsonProperty("snapshot_has_broken")
    private boolean snapshotHasBroken;

    protected String project;
    private DatabaseDesc database = new DatabaseDesc();
    private String identity = null;
    private boolean isBorrowedFromGlobal = false;
    private KafkaConfig kafkaConfig;

    public TableDesc() {
    }

    public TableDesc(TableDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.createTime = other.createTime;
        this.name = other.name;
        this.sourceType = other.sourceType;
        this.tableType = other.tableType;
        this.dataGen = other.dataGen;
        this.incrementLoading = other.incrementLoading;
        this.columns = new ColumnDesc[other.columns.length];
        for (int i = 0; i < other.columns.length; i++) {
            this.columns[i] = new ColumnDesc(other.columns[i]);
            this.columns[i].init(this);
        }
        this.isTop = other.isTop;
        this.project = other.project;
        this.database.setName(other.getDatabase());
        this.identity = other.identity;
        this.lastSnapshotPath = other.lastSnapshotPath;
        this.partitionColumn = other.partitionColumn;
        this.snapshotPartitions = other.snapshotPartitions;
        this.selectedSnapshotPartitionCol = other.selectedSnapshotPartitionCol;
        this.snapshotPartitionCol = other.snapshotPartitionCol;
        this.snapshotLastModified = other.getSnapshotLastModified();
        this.snapshotHasBroken = other.snapshotHasBroken;
        this.kafkaConfig = other.kafkaConfig;

        setMvcc(other.getMvcc());
    }

    @Override
    public String resourceName() {
        return getIdentity();
    }

    public TableDesc appendColumns(ColumnDesc[] computedColumns, boolean makeCopy) {
        if (computedColumns == null || computedColumns.length == 0) {
            return this;
        }

        TableDesc ret = makeCopy ? new TableDesc(this) : this;
        ColumnDesc[] existingColumns = ret.columns;
        List<ColumnDesc> newColumns = Lists.newArrayList();

        for (int j = 0; j < computedColumns.length; j++) {

            //check name conflict
            boolean isFreshCC = true;
            for (int i = 0; i < existingColumns.length; i++) {
                if (existingColumns[i].getName().equalsIgnoreCase(computedColumns[j].getName())) {
                    // if we're adding a computed column twice, it should be allowed without producing duplicates
                    if (!existingColumns[i].isComputedColumn()) {
                        throw new IllegalArgumentException(String.format(Locale.ROOT,
                                "There is already a column named %s on table %s, please change your computed column name",
                                computedColumns[j].getName(), this.getIdentity()));
                    } else {
                        isFreshCC = false;
                    }
                }
            }

            if (isFreshCC) {
                newColumns.add(computedColumns[j]);
            }
        }

        List<ColumnDesc> expandedColumns = Lists.newArrayList(existingColumns);
        for (ColumnDesc newColumnDesc : newColumns) {
            newColumnDesc.init(ret);
            expandedColumns.add(newColumnDesc);
        }
        ret.columns = expandedColumns.toArray(new ColumnDesc[0]);
        return ret;
    }

    public ColumnDesc findColumnByName(String name) {
        //ignore the db name and table name if exists
        int lastIndexOfDot = name.lastIndexOf(".");
        if (lastIndexOfDot >= 0) {
            name = name.substring(lastIndexOfDot + 1);
        }

        for (ColumnDesc c : columns) {
            // return first matched column
            if (name.equalsIgnoreCase(c.getOriginalName())) {
                return c;
            }
        }
        return null;
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getIdentity(), project);

    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.TABLE_RESOURCE_ROOT).append("/")
                .append(name).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getIdentity() {
        String originIdentity = getCaseSensitiveIdentity();
        return originIdentity.toUpperCase(Locale.ROOT);
    }

    public String getCaseSensitiveIdentity() {
        if (identity == null) {
            if (this.getCaseSensitiveDatabase().equals("null")) {
                identity = String.format(Locale.ROOT, "%s", this.getCaseSensitiveName());
            } else {
                identity = String.format(Locale.ROOT, "%s.%s", this.getCaseSensitiveDatabase(),
                        this.getCaseSensitiveName());
            }
        }
        return identity;
    }

    public boolean isView() {
        return StringUtils.containsIgnoreCase(tableType, TABLE_TYPE_VIEW);
    }

    public boolean isBorrowedFromGlobal() {
        return isBorrowedFromGlobal;
    }

    public void setBorrowedFromGlobal(boolean borrowedFromGlobal) {
        isBorrowedFromGlobal = borrowedFromGlobal;
    }

    public String getProject() {
        return project;
    }

    public String getName() {
        return name == null ? null : name.toUpperCase(Locale.ROOT);
    }

    @JsonGetter("name")
    public String getCaseSensitiveName() {
        return this.name;
    }

    @JsonSetter("name")
    public void setName(String name) {
        if (name != null) {
            String[] splits = StringSplitter.split(name, ".");
            if (splits.length == 2) {
                this.setDatabase(splits[0]);
                this.name = splits[1];
            } else if (splits.length == 1) {
                this.name = splits[0];
            }
            identity = null;
        } else {
            this.name = null;
        }
    }

    public String getDatabase() {
        return database.getName().toUpperCase(Locale.ROOT);
    }

    @JsonGetter("database")
    public String getCaseSensitiveDatabase() {
        return database.getName();
    }

    @JsonSetter("database")
    public void setDatabase(String database) {
        this.database.setName(database);
    }

    public int getMaxColumnIndex() {
        if (columns == null) {
            return -1;
        }

        int max = -1;

        for (ColumnDesc col : columns) {
            int idx = col.getZeroBasedIndex();
            max = Math.max(max, idx);
        }
        return max;
    }

    public int getColumnCount() {
        return getMaxColumnIndex() + 1;
    }

    public String getDataGen() {
        return dataGen;
    }

    public long getSnapshotLastModified() {
        if (snapshotLastModified == 0) {
            return lastModified;
        }
        return snapshotLastModified;
    }

    public void init(String project) {
        this.project = project;

        if (columns != null) {
            Arrays.sort(columns, (col1, col2) -> {
                Integer id1 = Integer.parseInt(col1.getId());
                Integer id2 = Integer.parseInt(col2.getId());
                return id1.compareTo(id2);
            });

            for (ColumnDesc col : columns) {
                col.init(this);
            }
        }
        kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getKafkaConfig(this.getIdentity());
    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TableDesc tableDesc = (TableDesc) o;

        if (sourceType != tableDesc.sourceType)
            return false;
        if (name != null ? !name.equals(tableDesc.name) : tableDesc.name != null)
            return false;
        if (!Arrays.equals(columns, tableDesc.columns))
            return false;

        return getIdentity().equals(tableDesc.getIdentity());

    }

    public String getMaterializedName() {
        return materializedTableNamePrefix + database.getName() + "_" + name;
    }

    @Override
    public String toString() {
        return "TableDesc{" + "name='" + name + '\'' + ", columns=" + Arrays.toString(columns) + ", sourceType="
                + sourceType + ", tableType='" + tableType + '\'' + ", database=" + database + ", identity='"
                + getIdentity() + '\'' + '}';
    }

    /** create a mockup table for unit test */
    public static TableDesc mockup(String tableName) {
        TableDesc mockup = new TableDesc();
        mockup.setName(tableName);
        return mockup;
    }

    @Override
    public KylinConfig getConfig() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(this.project);
        return projectInstance.getConfig();
    }

    @Override
    public int getSourceType() {
        return sourceType;
    }

    public void setSourceType(int sourceType) {
        this.sourceType = sourceType;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public void deleteSnapshot(boolean makeBroken) {
        this.lastSnapshotPath = null;
        snapshotPartitionCol = null;
        snapshotPartitions = Maps.newHashMap();
        selectedSnapshotPartitionCol = null;
        lastSnapshotSize = 0;
        snapshotHasBroken = makeBroken;
    }

    public void copySnapshotFrom(TableDesc originTable) {
        setLastSnapshotPath(originTable.getLastSnapshotPath());
        setSnapshotPartitions(originTable.getSnapshotPartitions());
        setSnapshotPartitionCol(originTable.getSnapshotPartitionCol());
        setSelectedSnapshotPartitionCol(originTable.getSelectedSnapshotPartitionCol());
        setSnapshotLastModified(originTable.getSnapshotLastModified());
        setSnapshotHasBroken(originTable.isSnapshotHasBroken());
    }

    public void resetSnapshotPartitions(Set<String> snapshotPartitions) {
        this.snapshotPartitions = snapshotPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), item -> -1L));
    }

    public void putPartitionSize(String partition, long size) {
        snapshotPartitions.put(partition, size);
    }

    public void addSnapshotPartitions(Set<String> snapshotPartitions) {
        snapshotPartitions.forEach(part -> this.snapshotPartitions.put(part, -1L));
    }

    public Set<String> getNotReadyPartitions() {
        Set<String> notReadyPartitions = Sets.newHashSet();
        snapshotPartitions.forEach((item, ready) -> {
            if (ready == -1) {
                notReadyPartitions.add(item);
            }
        });
        return notReadyPartitions;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }
}
