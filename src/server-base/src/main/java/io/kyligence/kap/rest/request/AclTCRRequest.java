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

package io.kyligence.kap.rest.request;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.acl.SensitiveDataMask;
import lombok.Data;

@Data
public class AclTCRRequest {
    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

    @Data
    public static class Table {
        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty
        private List<Column> columns;

        // Default value for rows, like_rows and row_filter is null
        // DO NOT CHANGE TO EMPTY LIST OR EMPTY ROW FILTER
        @JsonProperty
        private List<Row> rows;

        @JsonProperty("like_rows")
        private List<Row> likeRows;

        @JsonProperty("row_filter")
        private RowFilter rowFilter;
    }

    @Data
    public static class Column {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;
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
        private boolean group;

        @JsonProperty
        private List<Filter> filters = new ArrayList<>();
    }

    @Data
    public static class RowFilter {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("filter_groups")
        private List<FilterGroup> filterGroups = new ArrayList<>();
    }

    @Data
    public static class DependentColumnData {
        @JsonProperty("column_identity")
        private String columnIdentity;

        @JsonProperty("values")
        private String[] values;
    }
}
