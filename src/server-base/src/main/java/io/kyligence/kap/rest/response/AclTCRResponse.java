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

package io.kyligence.kap.rest.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.acl.DependentColumn;
import io.kyligence.kap.metadata.acl.SensitiveDataMask;
import lombok.Data;

@Data
public class AclTCRResponse {

    @JsonProperty("authorized_table_num")
    private int authorizedTableNum;

    @JsonProperty("total_table_num")
    private int totalTableNum;

    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

    @Data
    public static class Table {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty("authorized_column_num")
        private int authorizedColumnNum;

        @JsonProperty("total_column_num")
        private int totalColumnNum;

        @JsonProperty
        private List<Column> columns = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty
        private List<Row> rows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("like_rows")
        private List<Row> likeRows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("row_filter")
        private RowFilter rowFilter = new RowFilter();
    }

    @Data
    public static class Column {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;

        @JsonProperty("datatype")
        private String datatype;

        public void setDependentColumns(Collection<DependentColumn> dependentColumns) {
            this.dependentColumns = dependentColumns.stream()
                    .map(col -> new DependentColumnData(col.getDependentColumnIdentity(), col.getDependentValues()))
                    .collect(Collectors.toList());
        }
    }

    @Data
    public static class Row {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private List<String> items;
    }

    @Data
    public static class Filter {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("in_items")
        private List<String> inItems = new ArrayList<>();

        @JsonProperty("like_items")
        private List<String> likeItems = new ArrayList<>();
    }

    @Data
    public static class FilterGroup {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("is_group")
        private boolean group = false;

        @JsonProperty
        private List<AclTCRResponse.Filter> filters = new ArrayList<>();
    }

    @Data
    public static class RowFilter {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("filter_groups")
        private List<AclTCRResponse.FilterGroup> filterGroups = new ArrayList<>();
    }

    @Data
    public static class DependentColumnData {
        @JsonProperty("column_identity")
        private String columnIdentity;

        @JsonProperty("values")
        private String[] values;

        public DependentColumnData(String columnIdentity, String[] values) {
            this.columnIdentity = columnIdentity;
            this.values = values;
        }

        public DependentColumnData() {
        }
    }
}
