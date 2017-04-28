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

package io.kyligence.kap.source.hive.modelstats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ModelStats extends RootPersistentEntity {

    public static final String JOIN_RESULT_OVERALL = "join_result_overall";

    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("last_build_job_id")
    private String jodID;
    @JsonProperty("counter")
    private long counter;
    @JsonProperty("single_column_cardinality")
    private Map<String, Long> singleColumnCardinality = new HashMap<>();
    @JsonProperty("double_columns_cardinality")
    private Map<String, Long> doubleColumnCardinality = new HashMap<>();
    @JsonProperty("column_null")
    private Map<String, Long> columnNullMap = new HashMap<>();
    @JsonProperty("data_skew")
    private Map<String, List<SkewResult>> dataSkew = new HashMap<>();
    @JsonProperty("joint_result")
    private List<JoinResult> joinResult = new ArrayList<>();
    @JsonProperty("duplicate_primary_keys")
    private List<DuplicatePK> duplicatePrimaryKeys = new ArrayList<>();

    public ModelStats() {
    }

    public void setDuplicatePrimaryKeys(List<DuplicatePK> duplicatePrimaryKeys) {
        this.duplicatePrimaryKeys = duplicatePrimaryKeys;
    }

    public List<DuplicatePK> getDuplicatePrimaryKeys() {
        return this.duplicatePrimaryKeys;
    }

    public void setDataSkew(Map<String, List<SkewResult>> dataSkewFK) {
        this.dataSkew = dataSkewFK;
    }

    public Map<String, List<SkewResult>> getDataSkew() {
        return this.dataSkew;
    }

    public void setJoinResult(List<JoinResult> joinResult) {
        this.joinResult = joinResult;
    }

    public List<JoinResult> getJoinResult() {
        return joinResult;
    }

    public void setColumnNullMap(Map<String, Long> columnNullMap) {
        this.columnNullMap = columnNullMap;
    }

    public Map<String, Long> getColumnNullMap() {
        return this.columnNullMap;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public long getCounter() {
        return this.counter;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getModelName() {
        return this.modelName;
    }

    public void setJodID(String jobID) {
        this.jodID = jobID;
    }

    public String getJodID() {
        return this.jodID;
    }

    public void setSingleColumnCardinality(Map<String, Long> sCardinality) {
        this.singleColumnCardinality = sCardinality;
    }

    public Map<String, Long> getSingleColumnCardinality() {
        return this.singleColumnCardinality;
    }

    public void setDoubleColumnCardinality(Map<String, Long> dCardinality) {
        this.doubleColumnCardinality = dCardinality;
    }

    public Map<String, Long> getDoubleColumnCardinality() {
        return this.doubleColumnCardinality;
    }

    public long getSingleColumnCardinalityVal(String col) {
        if (singleColumnCardinality.containsKey(col)) {
            return singleColumnCardinality.get(col);
        } else {
            return -1;
        }
    }

    public long getDoubleColumnCardinalityVal(String col1, String col2) {
        Preconditions.checkNotNull(col1);
        Preconditions.checkNotNull(col2);

        String key = col1 + "," + col2;
        if (doubleColumnCardinality.containsKey(key)) {
            return doubleColumnCardinality.get(key);
        }

        key = col1 + "," + col1;
        if (doubleColumnCardinality.containsKey(key)) {
            return doubleColumnCardinality.get(key);
        }

        return -1;
    }

    public String getDuplicationResult() {
        StringBuilder ret = new StringBuilder();
        for (DuplicatePK dp : duplicatePrimaryKeys) {
            if (dp.getDuplication().size() > 0) {
                ret.append(dp.getLookUpTable());
                ret.append(",");
                ret.append(dp.getPrimaryKeys());
                ret.append(",");
                ret.append(dp.toString());
                ret.append(",");
                ret.append(dp.getDuplication());
                ret.append("\r\n");
            }
        }
        return ret.toString();
    }

    public String getJointResult() {
        StringBuilder ret = new StringBuilder();
        for (JoinResult e : joinResult) {
            ret.append(e.toString());
            ret.append("\r\n");
        }
        return ret.toString();
    }

    public String getSkewResult() {
        StringBuilder ret = new StringBuilder();
        for (Map.Entry<String, List<SkewResult>> e : dataSkew.entrySet()) {
            ret.append(e.getKey());
            ret.append(": ");
            for (SkewResult s : e.getValue()) {
                ret.append(s.toString());
            }
            ret.append("\r\n");
        }
        return ret.toString();
    }

    public String getResourcePath() {
        return ModelStatsManager.MODEL_STATISTICS_ROOT + "/" + modelName + MetadataConstants.FILE_SURFIX;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JoinResult implements Serializable {

        @JsonBackReference
        private ModelStats modelStats;

        @JsonProperty("join_table_name")
        private String joinTableName;

        @JsonProperty("primary_key")
        private String primaryKey;

        @JsonProperty("join_result_valid_count")
        private long joinResultValidCount;

        @JsonProperty("join_result_valid_ratio")
        private float joinResultValidRatio;

        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }

        public String getPrimaryKey() {
            return this.primaryKey;
        }

        public void setJoinTableName(String joinTableName) {
            this.joinTableName = joinTableName;
        }

        public String getJoinTableName() {
            return this.joinTableName;
        }

        public void setJoinResultValidCount(long joinResultValidCount) {
            this.joinResultValidCount = joinResultValidCount;
        }

        public long getJoinResultValidCount() {
            return this.joinResultValidCount;
        }

        public void setJoinResultRatio(float joinResultRatio) {
            this.joinResultValidRatio = joinResultRatio;
        }

        public float getJoinResultRatio() {
            return this.joinResultValidRatio;
        }

        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append(joinTableName);
            s.append(", ");
            s.append(primaryKey);
            s.append(", ");
            s.append(joinResultValidCount);
            s.append(", ");
            s.append(joinResultValidRatio);
            return s.toString();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SkewResult implements Serializable {

        @JsonBackReference
        private ModelStats modelStats;

        @JsonProperty("data_skew_value")
        private String dataSkewValue;

        @JsonProperty("data_skew_count")
        private long dataSkewCount;

        public void setDataSkewValue(String dataSkewValue) {
            this.dataSkewValue = dataSkewValue;
        }

        public String getDataSkewValue() {
            return this.dataSkewValue;
        }

        public void setDataSkewCount(long dataSkewCount) {
            this.dataSkewCount = dataSkewCount;
        }

        public long getDataSkewCount() {
            return this.dataSkewCount;
        }

        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append(dataSkewValue);
            s.append(", ");
            s.append(dataSkewCount);
            return s.toString();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DuplicatePK implements Serializable {

        @JsonBackReference
        private ModelStats modelStats;

        @JsonProperty("look_up_table")
        private String lookUpTable;

        @JsonProperty("primary_keys")
        private String primaryKeys;

        @JsonProperty("duplication")
        private Map<String, Integer> duplication;

        public void setLookUpTable(String lookUpTable) {
            this.lookUpTable = lookUpTable;
        }

        public String getLookUpTable() {
            return this.lookUpTable;
        }

        public void setPrimaryKeys(String primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        public String getPrimaryKeys() {
            return this.primaryKeys;
        }

        public void setDuplication(Map<String, Integer> duplication) {
            this.duplication = duplication;
        }

        public Map<String, Integer> getDuplication() {
            return this.duplication;
        }

        public String toString() {
            StringBuilder s = new StringBuilder();
            for (Map.Entry<String, Integer> e : this.duplication.entrySet()) {
                s.append(e.getKey());
                s.append(" : ");
                s.append(e.getValue());
                s.append(" ");
            }
            return s.toString();
        }
    }
}
