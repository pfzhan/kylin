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

package io.kyligence.kap.metadata.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

public class DimensionAdvisor {
    private KylinConfig kylinConfig;

    public DimensionAdvisor(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public Map<String, ColumnSuggestionType> inferDimensionSuggestions(String tableName, String prj) {
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(kylinConfig);

        Map<String, ColumnSuggestionType> result = new HashMap<String, ColumnSuggestionType>();
        TableDesc tableDesc = metadataManager.getTableDesc(tableName, prj);
        if (tableDesc == null)
            return result;

        ColumnDesc[] columns = tableDesc.getColumns();
        TableExtDesc tableExt = metadataManager.getTableExt(tableDesc);
        List<TableExtDesc.ColumnStats> columnStats = tableExt.getColumnStats();
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc column = columns[i];
            TableExtDesc.ColumnStats stat = columnStats.size() > i ? columnStats.get(i) : null;
            ColumnSuggestionType suggestion = inferDimensionSuggestion(column, stat);
            result.put(column.getName(), suggestion);
        }

        return result;
    }

    private ColumnSuggestionType inferDimensionSuggestion(ColumnDesc column, TableExtDesc.ColumnStats stat) {
        if (column.getType().isIntegerFamily()) {
            if (column.getName().toUpperCase().endsWith("ID") || column.getName().toUpperCase().endsWith("KEY")) {
                return ColumnSuggestionType.DIMENSION;
            }
            if (column.getType().isTinyInt() || column.getType().isSmallInt()) {
                return inferDimensionByCardinality(stat);
            } else {
                return ColumnSuggestionType.METRIC;
            }
        } else if (column.getType().isNumberFamily()) {
            return ColumnSuggestionType.METRIC;
        } else if (column.getType().isDateTimeFamily()) {
            if (column.getType().isDate() || column.getType().isDatetime()) {
                return inferDimensionByCardinality(stat);
            } else {
                return ColumnSuggestionType.METRIC;
            }
        } else if (column.getType().isStringFamily()) {
            return inferDimensionByCardinality(stat);
        } else {
            return inferDimensionByCardinality(stat);
        }
    }

    private ColumnSuggestionType inferDimensionByCardinality(TableExtDesc.ColumnStats stat) {
        if (stat == null) {
            return ColumnSuggestionType.DIMENSION;
        }
        long cardinality = stat.getCardinality();
        if (cardinality < 20) {
            return ColumnSuggestionType.DIMENSION_TINY;
        } else if (cardinality < 100) {
            return ColumnSuggestionType.DIMENSION_SMALL;
        } else if (cardinality < 1000) {
            return ColumnSuggestionType.DIMENSION_MEDIUM;
        } else if (cardinality < 10000) {
            return ColumnSuggestionType.DIMENSION_HIGH;
        } else if (cardinality < 100000) {
            return ColumnSuggestionType.DIMENSION_VERY_HIGH;
        } else {
            return ColumnSuggestionType.DIMENSION_ULTRA_HIGH;
        }
    }

    public enum ColumnSuggestionType {
        METRIC, // metric
        DIMENSION, // dimension without cardinality info
        DIMENSION_TINY, // cardinality<20
        DIMENSION_SMALL, //cardinality<100
        DIMENSION_MEDIUM, //cardinality<1,000
        DIMENSION_HIGH, //cardinality<10,000
        DIMENSION_VERY_HIGH, //cardinality<100,000
        DIMENSION_ULTRA_HIGH//cardinality>=100,000
    }
}
