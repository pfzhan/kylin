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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableExtDesc extends RootPersistentEntity {

    private static final HLLCSerializer HLLC_SERIALIZER = new HLLCSerializer(DataType.getType("hllc14"));

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

    @JsonProperty("table_name")
    private String tableIdentity;
    @JsonProperty("last_build_job_id")
    private String jodID;

    @JsonProperty("frequency")
    private int frequency;
    @JsonProperty("columns_stats")
    private List<ColumnStats> columnStats = new ArrayList<>();

    @JsonProperty("sample_rows")
    private List<String[]> sampleRows = new ArrayList<>();

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;
    @JsonProperty("total_rows")
    private long totalRows;
    @JsonProperty("mapper_rows")
    private List<Long> mapRecords = new ArrayList<>();
    @JsonProperty("data_source_properties")
    private Map<String, String> dataSourceProps = new HashMap<>();

    private String project;

    @JsonProperty("loading_range")
    private List<SegmentRange> loadingRange = new ArrayList<>();

    public TableExtDesc() {
    }

    public TableExtDesc(TableExtDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.tableIdentity = other.tableIdentity;
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
        return concatResourcePath(getIdentity(), getProject());
    }

    public void updateLoadingRange(final SegmentRange segmentRange) {
        loadingRange.add(segmentRange);
        Collections.sort(loadingRange);
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getIdentity() {
        return this.tableIdentity;
    }

    public String getJodID() {
        return this.jodID;
    }

    public void addDataSourceProp(String key, String value) {
        this.dataSourceProps.put(key, value);
    }

    public Map<String, String> getDataSourceProp() {
        return this.dataSourceProps;
    }

    public void setSampleRows(List<String[]> sampleRows) {
        this.sampleRows = sampleRows;
    }

    public List<String[]> getSampleRows() {
        return this.sampleRows;
    }

    public void setMapRecords(List<Long> mapRecords) {
        this.mapRecords = mapRecords;
    }

    public List<Long> getMapRecords() {
        return this.mapRecords;
    }

    public String getCardinality() {

        StringBuffer cardinality = new StringBuffer();
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

        if (0 == this.columnStats.size()) {
            for (int i = 0; i < cardi.length; i++) {
                ColumnStats columnStat = new ColumnStats();
                columnStat.setCardinality(Long.parseLong(cardi[i]));
                this.columnStats.add(columnStat);
            }
        } else if (this.columnStats.size() == cardi.length) {
            for (int i = 0; i < cardi.length; i++) {
                this.columnStats.get(i).setCardinality(Long.parseLong(cardi[i]));
            }
        } else {
            throw new IllegalArgumentException("The given cardinality columns don't match tables " + tableIdentity);

        }
    }

    public List<ColumnStats> getColumnStats() {
        return this.columnStats;
    }

    public ColumnStats getColumnStats(final int colIdx) {
        if (getColumnStats().size() > colIdx) {
            return getColumnStats().get(colIdx);
        }
        return null;
    }

    public void setColumnStats(List<ColumnStats> columnStats) {
        this.columnStats = null;
        this.columnStats = columnStats;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public long getTotalRows() {
        return this.totalRows;
    }

    public void setIdentity(String name) {
        this.tableIdentity = name;
    }

    public void setJodID(String jobID) {
        this.jodID = jobID;
    }

    public void init(String project) {
        this.project = project;
        if (this.tableIdentity != null)
            this.tableIdentity = this.tableIdentity.toUpperCase();

        for (final ColumnStats colStats : this.columnStats) {
            colStats.init();
        }
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    public boolean isPartitioned() {
        return this.dataSourceProps.get("partition_column") == null ? false
                : !this.dataSourceProps.get("partition_column").isEmpty();
    }

    public List<SegmentRange> getLoadingRange() {
        return loadingRange;
    }

    public void setLoadingRange(List<SegmentRange> loadingRange) {
        this.loadingRange = loadingRange;
    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "TableExtDesc{" + "name='" + (null == tableIdentity ? "NULL" : tableIdentity) + '\''
                + ", columns_samples=" + (null == columnStats ? "null" : Arrays.toString(columnStats.toArray()));
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

        @JsonProperty("range_hllc")
        private Map<String, byte[]> rangeHLLC = new HashMap<>();

        @JsonIgnore
        private transient HLLCounter totalHLLC;

        @JsonIgnore
        private transient long totalCardinality;

        @Override
        public int compareTo(ColumnStats o) {
            return 0;
        }

        public ColumnStats() {
        }

        void init() {
            if (rangeHLLC.isEmpty()) {
                return;
            }

            final Iterator<byte[]> hllcIterator = rangeHLLC.values().iterator();

            totalHLLC = HLLC_SERIALIZER.deserialize(ByteBuffer.wrap(hllcIterator.next()));
            while (hllcIterator.hasNext()) {
                totalHLLC.merge(HLLC_SERIALIZER.deserialize(ByteBuffer.wrap(hllcIterator.next())));
            }

            totalCardinality = totalHLLC.getCountEstimate();

            cardinality = totalCardinality;
        }

        public void addRangeHLLC(SegmentRange segRange, byte[] hllcBytes) {
            final String key = segRange.getStart() + "," + segRange.getEnd();
            rangeHLLC.put(key, hllcBytes);
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
    }
}
