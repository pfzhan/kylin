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

package org.apache.kylin.dict;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.source.IReadableTable.TableSignature;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NDictionaryInfo extends RootPersistentEntity {

    @JsonProperty("source_table")
    private String sourceTable;
    @JsonProperty("source_column")
    private String sourceColumn;
    @JsonProperty("source_column_index")
    private int sourceColumnIndex; // 0 based
    @JsonProperty("data_type")
    private String dataType;
    @JsonProperty("input")
    private TableSignature input;
    @JsonProperty("dictionary_class")
    private String dictionaryClass;
    @JsonProperty("cardinality")
    private int cardinality;

    private String project;
    transient Dictionary<String> dictionaryObject;

    public NDictionaryInfo() {
    }

    public NDictionaryInfo(ColumnDesc col, String dataType, TableSignature input, String project) {
        this(col.getTable().getIdentity(), col.getName(), col.getZeroBasedIndex(), dataType, input, project);
    }

    public NDictionaryInfo(String sourceTable, String sourceColumn, int sourceColumnIndex, String dataType,
            TableSignature input, String project) {

        this.updateRandomUuid();

        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.sourceColumnIndex = sourceColumnIndex;
        this.dataType = dataType;
        this.input = input;
        this.project = project;
    }

    public NDictionaryInfo(NDictionaryInfo other) {

        this.updateRandomUuid();

        this.sourceTable = other.sourceTable;
        this.sourceColumn = other.sourceColumn;
        this.sourceColumnIndex = other.sourceColumnIndex;
        this.dataType = other.dataType;
        this.input = other.input;
        this.project = other.project;
    }

    // ----------------------------------------------------------------------------

    public String getResourcePath() {
        return new StringBuilder().append(getResourceDir()).append('/').append(uuid).append(".dict").toString();
    }

    public String getResourceDir() {
        return new StringBuilder().append('/').append(project).append(ResourceStore.DICT_RESOURCE_ROOT).append('/')
                .append(sourceTable).append('/').append(sourceColumn).toString();
    }

    // ----------------------------------------------------------------------------

    // to decide if two dictionaries are built on the same table/column,
    // regardless of their signature
    public boolean isDictOnSameColumn(NDictionaryInfo other) {
        return this.sourceTable.equalsIgnoreCase(other.sourceTable)
                && this.sourceColumn.equalsIgnoreCase(other.sourceColumn)
                && this.sourceColumnIndex == other.sourceColumnIndex && this.dataType.equalsIgnoreCase(other.dataType)
                && this.dictionaryClass.equalsIgnoreCase(other.dictionaryClass);
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public int getSourceColumnIndex() {
        return sourceColumnIndex;
    }

    public void setSourceColumnIndex(int sourceColumnIndex) {
        this.sourceColumnIndex = sourceColumnIndex;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public TableSignature getInput() {
        return input;
    }

    public void setInput(TableSignature input) {
        this.input = input;
    }

    public String getDictionaryClass() {
        return dictionaryClass;
    }

    public void setDictionaryClass(String dictionaryClass) {
        this.dictionaryClass = dictionaryClass;
    }

    public Dictionary<String> getDictionaryObject() {
        return dictionaryObject;
    }

    public void setDictionaryObject(Dictionary<String> dictionaryObject) {
        this.dictionaryObject = dictionaryObject;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }
}
