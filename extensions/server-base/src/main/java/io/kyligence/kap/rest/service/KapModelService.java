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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

@Component("kapModelService")
public class KapModelService extends BasicService {

    public Map<HEALTH_STATUS, List<String>> getDiagnoseResult(String modelName) throws IOException {

        DiagnoseExtractor diagnoseExtractor = new DiagnoseExtractor(modelName);
        diagnoseExtractor.extract();
        return diagnoseExtractor.getFinalResult();
    }

    public Map<String, MODEL_COLUMN_SUGGESTION> inferSuggestions(String tableName) {
        Map<String, MODEL_COLUMN_SUGGESTION> result = new HashMap<String, MODEL_COLUMN_SUGGESTION>();
        TableDesc tableDesc = getMetadataManager().getTableDesc(tableName);
        if (tableDesc == null)
            return result;
        ColumnDesc[] columns = tableDesc.getColumns();
        TableExtDesc tableExt = getMetadataManager().getTableExt(tableName);
        List<TableExtDesc.ColumnStats> columnStats = tableExt.getColumnStats();
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc column = columns[i];
            TableExtDesc.ColumnStats stat = columnStats.size() > i ? columnStats.get(i) : null;
            MODEL_COLUMN_SUGGESTION suggestion = inferSuggestion(column, stat);
            result.put(column.getName(), suggestion);
        }

        return result;
    }

    private MODEL_COLUMN_SUGGESTION inferSuggestion(ColumnDesc column, TableExtDesc.ColumnStats stat) {
        if (column.getType().isIntegerFamily()) {
            if (column.getType().isTinyInt() || column.getType().isSmallInt()) {
                return inferDimensionByCardinality(stat);
            } else {
                return MODEL_COLUMN_SUGGESTION.MEASURE;
            }
        } else if (column.getType().isNumberFamily()) {
            return MODEL_COLUMN_SUGGESTION.MEASURE;
        } else if (column.getType().isDateTimeFamily()) {
            if (column.getType().isDate() || column.getType().isDatetime()) {
                return inferDimensionByCardinality(stat);
            } else {
                return MODEL_COLUMN_SUGGESTION.MEASURE;
            }
        } else if (column.getType().isStringFamily()) {
            return inferDimensionByCardinality(stat);
        } else {
            return inferDimensionByCardinality(stat);
        }
    }

    private MODEL_COLUMN_SUGGESTION inferDimensionByCardinality(TableExtDesc.ColumnStats stat) {
        if (stat == null) {
            return MODEL_COLUMN_SUGGESTION.DIMENSION;
        }
        long cardinality = stat.getCardinality();
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
        DIMENSION, // dimension without cardinality info
        DIMENSION_TINY, // cardinality<20
        DIMENSION_SMALL, //cardinality<100
        DIMENSION_MEDIUM, //cardinality<1,000
        DIMENSION_HIGH, //cardinality<10,000
        DIMENSION_VERY_HIGH, //cardinality<100,000
        DIMENSION_ULTRA_HIGH//cardinality>=100,000
    }

    public enum HEALTH_STATUS {
        GOOD, WARN, BAD, TERRIBLE
    }

    public class DiagnoseExtractor {
        private final static String DATA_SKEW = "data_skew";
        private final static String JOIN_RET = "flat_table_join_result";
        private final static String DUP_PK = "lookup table's duplicate PKs";

        private HEALTH_STATUS heathStatus = HEALTH_STATUS.GOOD;

        private ModelStats modelStats;
        private List<ItemInfo> itemStatsList = new ArrayList<>();

        public DiagnoseExtractor(String modelName) throws IOException {
            modelStats = ModelStatsManager.getInstance(getConfig()).getModelStats(modelName);
        }

        public Map<HEALTH_STATUS, List<String>> getFinalResult() {
            Map<HEALTH_STATUS, List<String>> ret = new HashMap<>();
            List<String> l = new ArrayList<>();
            for (ItemInfo e : itemStatsList) {
                l.add(e.toString());
            }
            ret.put(heathStatus, l);
            return ret;
        }

        public void extract() {
            extract(DUP_PK, modelStats.getDuplicationResult());
            extract(DATA_SKEW, modelStats.getSkewResult());
            extract(JOIN_RET, modelStats.getJointResult());
        }

        public void extract(String type, String ret) {
            ItemInfo item = new ItemInfo();
            item.setType(type);

            int sign = 0;
            if (!StringUtils.isEmpty(ret)) {
                item.setDetail(ret);
                itemStatsList.add(item);
                sign++;
            }
            judgeHealthStatus(sign);
        }

        public void judgeHealthStatus(int sign) {
            switch (sign) {
            case 0:
                heathStatus = HEALTH_STATUS.GOOD;
                break;
            case 1:
                heathStatus = HEALTH_STATUS.WARN;
                break;
            case 2:
                heathStatus = HEALTH_STATUS.BAD;
                break;
            case 3:
                heathStatus = HEALTH_STATUS.TERRIBLE;
                break;
            default:
                break;
            }
        }
    }

    public class ItemInfo {
        private String type;
        private String detail;

        public ItemInfo() {
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.getType();
        }

        public void setDetail(String detail) {
            this.detail = detail;
        }

        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append(type);
            s.append(":");
            s.append(detail);
            return s.toString();
        }
    }
}
