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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TableDescResponse extends TableDesc {
    @JsonProperty("exd")
    private Map<String, String> descExd = new HashMap<>();
    @JsonProperty("root_fact")
    private boolean rootFact;
    @JsonProperty("lookup")
    private boolean lookup;
    @JsonProperty("primary_key")
    private Set<String> primaryKey = new HashSet<>();
    @JsonProperty("foreign_key")
    private Set<String> foreignKey = new HashSet<>();
    @JsonProperty("partitioned_column")
    private String partitionedColumn;
    @JsonProperty("partitioned_column_format")
    private String partitionedColumnFormat;
    @JsonProperty("segment_range")
    private SegmentRange segmentRange;
    @JsonProperty("storage_size")
    private long storageSize = -1;
    @JsonProperty("total_records")
    private long totalRecords;
    @JsonProperty("sampling_rows")
    private List<String[]> samplingRows = new ArrayList<>();
    @JsonProperty("columns")
    private ColumnDescResponse[] extColumns;
    @JsonProperty("last_build_job_id")
    private String jodID;

    @JsonProperty("kafka_bootstrap_servers")
    private String kafkaBootstrapServers;
    @JsonProperty("subscribe")
    private String subscribe;
    @JsonProperty("batch_table_identity")
    private String batchTable;
    @JsonProperty("parser_name")
    private String parserName;

    public TableDescResponse(TableDesc table) {
        super(table);
        extColumns = new ColumnDescResponse[getColumns().length];
        for (int i = 0; i < getColumns().length; i++) {
            extColumns[i] = new ColumnDescResponse(getColumns()[i]);
        }
    }

    @Getter
    @Setter
    public class ColumnDescResponse extends ColumnDesc {
        @JsonProperty("cardinality")
        private Long cardinality;
        @JsonProperty("min_value")
        private String minValue;
        @JsonProperty("max_value")
        private String maxValue;
        @JsonProperty("null_count")
        private Long nullCount;

        ColumnDescResponse(ColumnDesc col) {
            super(col);
        }
    }

}