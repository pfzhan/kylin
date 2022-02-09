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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SnapshotColResponse implements Comparable<SnapshotColResponse> {

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("partition_col")
    private String partitionCol;

    @JsonProperty("partition_col_type")
    private String partitionColType;

    @JsonProperty("other_column_and_type")
    private Map<String, String> otherCol;

    @JsonProperty("source_type")
    private int sourceType;

    public SnapshotColResponse(String database, String table, String partitionCol, String partitionColType,
            Map<String, String> otherCol, int sourceType) {
        this.database = database;
        this.table = table;
        this.partitionCol = partitionCol;
        this.partitionColType = partitionColType;
        this.otherCol = otherCol;
        this.sourceType = sourceType;
    }

    public static SnapshotColResponse from(TableDesc table) {
        if (table.getPartitionColumn() != null) {

            ColumnDesc partCol = table.findColumnByName(table.getPartitionColumn());
            return new SnapshotColResponse(table.getDatabase(), table.getName(), partCol.getName(),
                    partCol.getDatatype(), excludePartCol(table.getColumns(), partCol.getName()),
                    table.getSourceType());
        } else {
            return new SnapshotColResponse(table.getDatabase(), table.getName(), null, null,
                    excludePartCol(table.getColumns(), null), table.getSourceType());
        }
    }

    private static Map<String, String> excludePartCol(ColumnDesc[] columns, String partitionCol) {
        return Arrays.asList(columns).stream().filter(col -> !col.getName().equals(partitionCol))
                .collect(Collectors.toMap(ColumnDesc::getName, ColumnDesc::getDatatype));

    }

    @Override
    public int compareTo(SnapshotColResponse other) {
        if (this.database.compareTo(other.database) != 0) {
            return this.database.compareTo(other.database);
        }
        return this.table.compareTo(other.table);
    }
}
