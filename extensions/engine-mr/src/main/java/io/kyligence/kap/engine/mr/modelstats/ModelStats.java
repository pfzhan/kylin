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

package io.kyligence.kap.engine.mr.modelstats;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ModelStats extends RootPersistentEntity {

    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("last_build_job_id")
    private String jodID;
    @JsonProperty("column_index_map")
    private Map<String, Integer> columnIndexMap = new HashMap<>();
    @JsonProperty("single_column_cardinality")
    private Map<Integer, Long> singleColumnCardinality = new HashMap<>();
    @JsonProperty("double_columns_cardinality")
    private Map<String, Long> doubleColumnCardinality = new HashMap<>();

    public ModelStats() {
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

    public void setColumnIndexMap(Map<String, Integer> columnIndexMap) {
        this.columnIndexMap = columnIndexMap;
    }

    public Map<String, Integer> getColumnIndexMap() {
        return this.columnIndexMap;
    }

    public void setSingleColumnCardinality(Map<Integer, Long> sCardinality) {
        this.singleColumnCardinality = sCardinality;
    }

    public void appendSingleColumnCardinality(Map<Integer, Long> sCardinality) {
        this.singleColumnCardinality.putAll(sCardinality);
    }

    public Map<Integer, Long> getSingleColumnCardinality() {
        return this.singleColumnCardinality;
    }

    public void setDoubleColumnCardinality(Map<String, Long> dCardinality) {
        this.doubleColumnCardinality = dCardinality;
    }

    public void appendDoubleColumnCardinality(Map<String, Long> dCardinality) {
        this.doubleColumnCardinality.putAll(dCardinality);
    }

    public Map<String, Long> getDoubleColumnCardinality() {
        return this.doubleColumnCardinality;
    }

    public Long getSingleCardByIndex(int index) {
        return this.singleColumnCardinality.get(index);
    }

    public Long getDoubleCardByIndex(int index1, int index2) {
        if (index1 == index2)
            return singleColumnCardinality.get(index1);

        StringBuilder key = new StringBuilder();
        key.append(Math.min(index1, index2));
        key.append(",");
        key.append(Math.max(index1, index2));
        return this.doubleColumnCardinality.get(key.toString());
    }

    public Long getSingleCardByColumnName(String columnName) {
        return this.singleColumnCardinality.get(columnIndexMap.get(columnName));
    }

    public Long getDoubleCardByColumnName(String columnName1, String columnName2) {
        int index1 = columnIndexMap.get(columnName1);
        int index2 = columnIndexMap.get(columnName2);
        return getDoubleCardByIndex(index1, index2);
    }

    public String getResourcePath() {
        return ModelStatsManager.MODEL_STATISTICS_ROOT + "/" + modelName + MetadataConstants.FILE_SURFIX;
    }
}
