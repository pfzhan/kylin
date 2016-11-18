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

package io.kyligence.kap.rest.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

@Component("kapModelService")
public class KapModelService extends BasicService {

    public Map<ColumnDesc, MODEL_COLUMN_SUGGESTION> inferSuggestions(String tableName) {
        Map<ColumnDesc, MODEL_COLUMN_SUGGESTION> result = new HashMap<ColumnDesc, MODEL_COLUMN_SUGGESTION>();
        TableDesc tableDesc = getMetadataManager().getTableDesc(tableName);
        ColumnDesc[] columns = tableDesc.getColumns();
        TableExtDesc tableExt = getMetadataManager().getTableExt(tableName);
        List<TableExtDesc.ColumnStats> columnStats = tableExt.getColumnStats();
        if (columnStats.size() != columns.length) {
            // No Stats Data
            return result;
        }
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc column = columns[i];
            TableExtDesc.ColumnStats stat = columnStats.get(i);
            MODEL_COLUMN_SUGGESTION suggestion = inferSuggestion(column, stat);
            result.put(column, suggestion);
        }

        return result;
    }

    private MODEL_COLUMN_SUGGESTION inferSuggestion(ColumnDesc column, TableExtDesc.ColumnStats stat) {
        if (column.getType().isIntegerFamily()) {
            if (column.getType().isTinyInt() || column.getType().isSmallInt()) {
                return inferDimensionByCardinality(stat.getCardinality());
            } else {
                return MODEL_COLUMN_SUGGESTION.MEASURE;
            }
        } else if (column.getType().isNumberFamily()) {
            return MODEL_COLUMN_SUGGESTION.MEASURE;
        } else if (column.getType().isDateTimeFamily()) {
            if (column.getType().isDate() || column.getType().isDatetime()) {
                return inferDimensionByCardinality(stat.getCardinality());
            } else {
                return MODEL_COLUMN_SUGGESTION.MEASURE;
            }
        } else if (column.getType().isStringFamily()) {
            return inferDimensionByCardinality(stat.getCardinality());
        } else {
            return inferDimensionByCardinality(stat.getCardinality());
        }
    }

    private MODEL_COLUMN_SUGGESTION inferDimensionByCardinality(long cardinality) {
        if (cardinality < 20) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_TINY;
        } else if (cardinality < 100) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_SMALL;
        } else if (cardinality < 1000) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_MEDIUM;
        } else if (cardinality < 10000) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_HIGH;
        } else if (cardinality < 100000) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_VERY_HIGH;
        } else {
            return MODEL_COLUMN_SUGGESTION.DIMENSION_ULTRA_HIGH;
        }
    }

    public enum MODEL_COLUMN_SUGGESTION {
        MEASURE, // measure
        DIMENSION_TINY, // cardinality<20
        DIMENSION_SMALL, //cardinality<100
        DIMENSION_MEDIUM, //cardinality<1,000
        DIMENSION_HIGH, //cardinality<10,000
        DIMENSION_VERY_HIGH, //cardinality<100,000
        DIMENSION_ULTRA_HIGH//cardinality>=100,000
    }
}
