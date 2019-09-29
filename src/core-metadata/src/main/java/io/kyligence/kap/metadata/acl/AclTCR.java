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

package io.kyligence.kap.metadata.acl;

import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.scheduler.SchedulerEventNotifier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class AclTCR extends RootPersistentEntity implements IKeep {

    //wrap read only aclTCR

    private String resourceName;

    public void init(String resourceName) {
        this.resourceName = resourceName;
    }

    @JsonProperty
    private Table table = null;

    @Override
    public String resourceName() {
        return resourceName;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isAuthorized(String dbTblName) {
        final Table table = this.table;
        if (Objects.isNull(table)) {
            return true;
        }
        return table.containsKey(dbTblName);
    }

    public boolean isAuthorized(String dbTblName, String columnName) {
        final Table table = this.table;
        if (Objects.isNull(table)) {
            return true;
        }
        if (!table.containsKey(dbTblName)) {
            return false;
        }
        if (Objects.isNull(table.get(dbTblName)) || Objects.isNull(table.get(dbTblName).getColumn())) {
            return true;
        }
        return table.get(dbTblName).getColumn().contains(columnName);
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Table extends TreeMap<String, ColumnRow> implements IKeep {

        // # { DB.TABLE1: { "columns": ["COL1","COL2","COL3"], "rows":{COL1:["A","B","C"]} } } #
        public Table() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnRow implements IKeep {

        @JsonProperty
        private Column column = null;

        @JsonProperty
        private Row row = null;

        public Column getColumn() {
            return column;
        }

        public void setColumn(Column column) {
            this.column = column;
        }

        public Row getRow() {
            return row;
        }

        public void setRow(Row row) {
            this.row = row;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Column extends TreeSet<String> implements IKeep {

        // ["COL1", "COL2", "COL3"]
        public Column() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Row extends TreeMap<String, RealRow> implements IKeep {

        // # { COL1: [ "A", "B", "C" ] } #
        public Row() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class RealRow extends TreeSet<String> implements IKeep {

        // ["A", "B", "C"]
        public RealRow() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ChangeEvent extends SchedulerEventNotifier {
        public ChangeEvent(String project, String subject) {
            this.project = project;
            this.subject = subject;
        }
    }
}
