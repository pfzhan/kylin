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
package io.kyligence.kap.engine.spark.source;

import java.io.Serializable;
import java.util.List;

import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;

import com.google.common.collect.Lists;

public class NSparkTableMetaExplorer implements Serializable {

    private Dataset<Row> getTableDesc(String database, String tableName) {
        String sql = Strings.isNullOrEmpty(database) ? String.format("describe formatted %s", tableName)
                : String.format("describe formatted %s.%s", database, tableName);
        return SparderEnv.getSparkSession().sql(sql);
    }

    public NSparkTableMeta getSparkTableMeta(String database, String tableName) {
        Dataset<Row> tableDesc = getTableDesc(database, tableName);

        NSparkTableMetaBuilder builder = new NSparkTableMetaBuilder();
        builder.setTableName(tableName);
        extractSparkTableMeta(tableDesc, builder);
        return builder.createSparkTableMeta();
    }

    private void extractSparkTableMeta(Dataset<Row> tableDesc, NSparkTableMetaBuilder builder) {
        boolean isColumn = true;

        List<Row> rowList = tableDesc.collectAsList();
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists.newArrayList();

        for (int i = 0; i < rowList.size(); i++) {
            Row row = rowList.get(i);
            if (row.getString(0) == null || "".equals(row.getString(0).trim())) {
                isColumn = false;
                continue;
            }
            if ("# Detailed Table ..".equals(row.getString(0).trim())) {
                isColumn = false;
                continue;
            }
            if (isColumn) {
                allColumns.add(
                        new NSparkTableMeta.SparkTableColumnMeta(row.getString(0), row.getString(1), row.getString(2)));
            } else {
                if ("Owner".equals(row.getString(0).trim())) {
                    builder.setOwner(row.getString(1).trim());
                }
                if ("Created".equals(row.getString(0).trim())) {
                    builder.setCreateTime(row.getString(1).trim());
                }
                if ("Last Access".equals(row.getString(0).trim())) {
                    builder.setLastAccessTime(row.getString(1).trim());
                }
                if ("Type".equals(row.getString(0).trim())) {
                    builder.setTableType(row.getString(1).trim());
                }
                if ("Provider".equals(row.getString(0).trim())) {
                    builder.setProvider(row.getString(1).trim());
                }
                //[totalSize=200000, COLUMN_STATS_ACCURATE=true, numFiles=1, transient_lastDdlTime=1509004553]
                if ("Table Properties".equals(row.getString(0).trim())) {
                    String tableProperties = row.getString(1);
                    String[] properties = tableProperties.split(",");
                    for (int j = 0; j < properties.length; j++) {
                        if (properties[j].contains("totalSize")) {
                            try {
                                builder.setFileSize(
                                        Long.parseLong(properties[j].substring(properties[j].indexOf("=") + 1)));
                            } catch (Exception e) {
                                builder.setFileSize(0);
                            }
                        }
                        if (properties[j].contains("numFiles")) {
                            try {
                                builder.setFileNum(
                                        Long.parseLong(properties[j].substring(properties[j].indexOf("=") + 1)));
                            } catch (Exception e) {
                                builder.setFileNum(0);
                            }
                        }
                    }
                }
                if ("Location".equals(row.getString(0).trim())) {
                    builder.setSdLocation(row.getString(1).trim());
                }
                if ("InputFormat".equals(row.getString(0).trim())) {
                    builder.setSdInputFormat(row.getString(1).trim());
                }
                if ("OutputFormat".equals(row.getString(0).trim())) {
                    builder.setSdOutputFormat(row.getString(1).trim());
                }

            }
        }
        builder.setAllColumns(allColumns);
    }
}
