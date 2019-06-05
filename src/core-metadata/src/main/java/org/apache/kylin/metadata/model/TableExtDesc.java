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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableExtDesc extends RootPersistentEntity implements Serializable {

    public static String concatRawResourcePath(String nameOnPath) {
        return ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + nameOnPath + ".json";
    }

    public static String concatResourcePath(String tableIdentity, String prj) {
        return concatRawResourcePath(TableDesc.makeResourceName(tableIdentity, prj));
    }

    // returns <table, project>
    public static Pair<String, String> parseResourcePath(String path) {
        return TableDesc.parseResourcePath(path);
    }

    // ============================================================================

    @Getter
    @Setter
    @JsonProperty("table_name")
    private String identity;

    @Getter
    @Setter
    @JsonProperty("last_build_job_id")
    private String jodID;

    @Getter
    @Setter
    @JsonProperty("frequency")
    private int frequency;

    @Setter
    @JsonProperty("columns_stats")
    private List<ColumnStats> columnStats = new ArrayList<>(); // should not expose getter

    @Getter
    @Setter
    @JsonProperty("sample_rows")
    private List<String[]> sampleRows = new ArrayList<>();

    @Getter
    @Setter
    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

    @Getter
    @Setter
    @JsonProperty("total_rows")
    private long totalRows;

    @Setter
    @JsonProperty("mapper_rows")
    private List<Long> mapRecords = new ArrayList<>();

    @Getter
    @JsonProperty("data_source_properties")
    private Map<String, String> dataSourceProps = new HashMap<>();

    @Getter
    private String project;

    @Getter
    @Setter
    @JsonProperty("loading_range")
    private List<SegmentRange> loadingRange = new ArrayList<>();

    @Setter
    @Getter
    @JsonProperty("col_stats_path")
    private String colStatsPath;

    public TableExtDesc() {
    }

    public TableExtDesc(TableExtDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.identity = other.identity;
        this.jodID = other.jodID;
        this.frequency = other.frequency;
        this.columnStats = other.columnStats;
        this.sampleRows = other.sampleRows;
        this.lastModifiedTime = other.lastModifiedTime;
        this.totalRows = other.totalRows;
        this.mapRecords = other.mapRecords;
        this.dataSourceProps = other.dataSourceProps;
        this.project = other.project;
    }

    @Override
    public String resourceName() {
        return getIdentity();
    }

    @Override
    public String getResourcePath() {
        return new StringBuilder().append("/").append(getProject()).append(ResourceStore.TABLE_EXD_RESOURCE_ROOT)
                .append("/").append(getIdentity()).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public void updateLoadingRange(final SegmentRange segmentRange) {
        loadingRange.add(segmentRange);
        Collections.sort(loadingRange);
    }

    public void addDataSourceProp(String key, String value) {
        this.dataSourceProps.put(key, value);
    }

    public String getCardinality() {

        StringBuilder cardinality = new StringBuilder();
        for (ColumnStats stat : this.columnStats) {
            cardinality.append(stat.getCardinality());
            cardinality.append(",");
        }
        return cardinality.toString();
    }

    public void resetCardinality() {
        int columnSize = this.columnStats.size();
        this.columnStats.clear();
        for (int i = 0; i < columnSize; i++) {
            this.columnStats.add(new ColumnStats());
        }
    }

    public void setCardinality(String cardinality) {
        if (null == cardinality)
            return;

        String[] cardi = cardinality.split(",");

        if (this.columnStats.isEmpty()) {
            for (String aCardi : cardi) {
                ColumnStats columnStat = new ColumnStats();
                columnStat.setCardinality(Long.parseLong(aCardi));
                this.columnStats.add(columnStat);
            }
        } else if (this.columnStats.size() == cardi.length) {
            for (int i = 0; i < cardi.length; i++) {
                this.columnStats.get(i).setCardinality(Long.parseLong(cardi[i]));
            }
        } else {
            throw new IllegalArgumentException("The given cardinality columns don't match tables " + identity);
        }
    }

    /**
     * Get all column stats info of a table. Owing to the side effect of schema change,
     * it may get an error result when making use of this method to get stats info of
     * a specified column indirectly. Instead, you can use {@link #getColumnStatsByName(java.lang.String)}
     * directly to get column stats info.
     */
    public List<ColumnStats> getAllColumnStats() {
        return columnStats;
    }

    /**
     * Get stats info of specified column by column name.
     */
    public ColumnStats getColumnStatsByName(String colName) {
        Map<String, ColumnStats> columnStatsMap = Maps.newHashMap();
        for (ColumnStats col : columnStats) {
            columnStatsMap.putIfAbsent(col.getColumnName(), col);
        }

        return columnStatsMap.getOrDefault(colName, null);
    }

    public void init(String project) {
        this.project = project;
        if (this.identity != null)
            this.identity = this.identity.toUpperCase();
    }

    public boolean isPartitioned() {
        return this.dataSourceProps.get("partition_column") != null
                && !this.dataSourceProps.get("partition_column").isEmpty();
    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        return "TableExtDesc{" + "name='" + (null == identity ? "NULL" : identity) + '\'' + ", columns_samples="
                + (null == columnStats ? "null" : Arrays.toString(columnStats.toArray()));
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnStats implements Comparable<ColumnStats>, Serializable {

        @JsonBackReference
        private TableExtDesc tableExtDesc;

        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("max_numeral")
        private double maxNumeral = Double.NaN;

        @JsonProperty("min_numeral")
        private double minNumeral = Double.NaN;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_length")
        private Integer maxLength;

        @JsonProperty("min_length")
        private Integer minLength;

        @JsonProperty("max_length_value")
        private String maxLengthValue;

        @JsonProperty("min_length_value")
        private String minLengthValue;

        @JsonProperty("null_count")
        private long nullCount;

        @JsonProperty("exceed_precision_count")
        private long exceedPrecisionCount;

        @JsonProperty("exceed_precision_max_length_value")
        private String exceedPrecisionMaxLengthValue;

        @JsonProperty("cardinality")
        private long cardinality;

        @JsonProperty("data_skew_samples")
        private Map<String, Long> dataSkewSamples = new HashMap<>();

        @JsonIgnore
        private transient Map<String, HLLCounter> rangeHLLC = new HashMap<>();

        @JsonIgnore
        private transient HLLCounter totalHLLC;

        @JsonIgnore
        private transient long totalCardinality;

        @Override
        public int compareTo(ColumnStats o) {
            return 0;
        }

        public void init() {
            if (rangeHLLC.isEmpty()) {
                return;
            }

            final Iterator<HLLCounter> hllcIterator = rangeHLLC.values().iterator();

            totalHLLC = new HLLCounter(hllcIterator.next());
            while (hllcIterator.hasNext()) {
                totalHLLC.merge(hllcIterator.next());
            }

            totalCardinality = totalHLLC.getCountEstimate();

            cardinality = totalCardinality;
        }

        public void addRangeHLLC(SegmentRange segRange, HLLCounter hllc) {
            final String key = segRange.getStart() + "_" + segRange.getEnd();
            rangeHLLC.put(key, hllc);
        }

        public void addRangeHLLC(String segRange, HLLCounter hllc) {
            rangeHLLC.put(segRange, hllc);
        }

        public void updateBasicStats(double maxNumeral, double minNumeral, int maxLength, int minLength,
                String maxLengthValue, String minLengthValue) {
            if (Double.isNaN(this.maxNumeral) || maxNumeral > this.maxNumeral) {
                this.maxNumeral = maxNumeral;
            }

            if (Double.isNaN(this.minNumeral) || minNumeral < this.minNumeral) {
                this.minNumeral = minNumeral;
            }

            if (this.maxLength == null || maxLength > this.maxLength) {
                this.maxLength = maxLength;
                this.maxLengthValue = maxLengthValue;
            }

            if (this.minLength == null || minLength < this.minLength) {
                this.minLength = minLength;
                this.minLengthValue = minLengthValue;
            }
        }

        @JsonIgnore
        public long getTotalCardinality() {
            return totalCardinality;
        }

        public void addNullCount(long incre) {
            this.nullCount += incre;
        }

        public void setColumnSamples(String max, String min, String maxLenValue, String minLenValue) {
            this.maxValue = max;
            this.minValue = min;
            this.maxLengthValue = maxLenValue;
            this.minLengthValue = minLenValue;
        }

        public static TableExtDesc.ColumnStats getColumnStats(NTableMetadataManager tableMetadataManager,
                TblColRef colRef) {
            TableExtDesc.ColumnStats ret = null;

            TableExtDesc tableExtDesc = tableMetadataManager.getTableExtIfExists(colRef.getTableRef().getTableDesc());
            if (tableExtDesc != null) {
                ret = tableExtDesc.getColumnStatsByName(colRef.getColumnDesc().getName());
            }
            return ret;
        }
    }
}
