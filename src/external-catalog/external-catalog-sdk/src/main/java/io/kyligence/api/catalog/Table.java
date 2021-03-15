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
package io.kyligence.api.catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kyligence.api.annotation.InterfaceStability.Evolving;

@Evolving
public class Table {

    public enum Format {
        JSON, CSV, PARQUET, ORC
    }

    /**
     * Typesafe enum for types of tables described by the metastore.
     */
    public enum Type {
        EXTERNAL_TABLE, VIEW
    }

    public static class StorageDescriptor {
        String path;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    private List<FieldSchema> partitionColumnNames; // required
    private final String tableName; // required
    private final String dbName; // required
    private String owner; // required
    private int createTime; // required
    private int lastAccessTime; // required
    private String format;
    private final StorageDescriptor sd; // required
    private List<FieldSchema> fields; // required fields can not contain partition col
    private final Map<String, String> parameters; // required
    private String viewText;
    private String tableType;

    public Table(String tableName, String dbName) {
        this.tableName = tableName;
        this.dbName = dbName;
        this.sd = new StorageDescriptor();
        this.parameters = new HashMap<>();
    }

    public List<FieldSchema> getPartitionColumnNames() {
        return partitionColumnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getLastAccessTime() {
        return lastAccessTime;
    }

    public void setPartitionColumnNames(List<FieldSchema> partitionColumnNames) {
        this.partitionColumnNames = partitionColumnNames;
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format.name();
    }

    public List<FieldSchema> getFields() {
        return fields;
    }

    public void setFields(List<FieldSchema> fields) {
        this.fields = fields;
    }

    public StorageDescriptor getSd() {
        return sd;
    }

    public void setProperty(String name, String value) {
        parameters.put(name, value);
    }

    public String getProperty(String name) {
        return parameters.get(name);
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getViewText() {
        return viewText;
    }

    public void setViewText(String viewText) {
        this.viewText = viewText;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(Type tableType) {
        this.tableType = tableType.name();
    }
}
