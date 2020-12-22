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
package io.kyligence.kap.source.jdbc;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSourceInput implements NSparkCubingEngine.NSparkCubingSource {
    @Override
    public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {
        ColumnDesc[] columnDescs = table.getColumns();
        List<String> tblColNames = Lists.newArrayListWithCapacity(columnDescs.length);
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : columnDescs) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(),
                        SparderTypeUtil.toSparkType(columnDesc.getType(), false), true);
                tblColNames.add(columnDesc.getName());
            }
        }
        String[] colNames = tblColNames.toArray(new String[0]);
        String colString = Joiner.on(",").join(colNames);
        String sql = String.format(Locale.ROOT, "select %s from %s", colString, table.getIdentity());
        KylinConfig config = table.getConfig();
        ISourceConnector connector = (ISourceConnector) ClassUtil.newInstance(config.getJdbcSourceConnector());
        log.info("Use source connector: {}", connector.getClass().getCanonicalName());
        Dataset<Row> dataset = connector.getSourceData(config, ss, sql);
        StructType sparkSchema = dataset.schema();
        return dataset.select(SparderTypeUtil.alignDataType(sparkSchema, kylinSchema));
    }
}
