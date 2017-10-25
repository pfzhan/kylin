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

package io.kyligence.kap.storage.parquet.adhoc;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class PushDownRunnerSparkImpl implements IPushDownRunner {
    public static final Logger logger = LoggerFactory.getLogger(PushDownRunnerSparkImpl.class);

    private SparkDriverClient client;

    @Override
    public void init(KylinConfig config) {
        try {
            client = new SparkDriverClient(KapConfig.getInstanceFromEnv());
        } catch (Exception e) {
            logger.error("error is " + e.getLocalizedMessage());
            throw e;
        }
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas)
            throws Exception {
        SparkJobProtos.PushDownResponse response = client.queryWithPushDown(query);
        int columnCount = response.getColumnsCount();
        List<SparkJobProtos.StructField> fieldList = response.getColumnsList();

        for (SparkJobProtos.Row row : response.getRowsList()) {
            List<String> dataRow = new ArrayList<>();

            for (SparkJobProtos.Row.Cell elem : row.getCellList()) {
                if (elem.hasValue()) {
                    dataRow.add(elem.getValue());
                } else {
                    dataRow.add(null);
                }
            }
            results.add(dataRow);
        }

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).getNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true, Integer.MAX_VALUE,
                    fieldList.get(i).getName().toUpperCase(), fieldList.get(i).getName().toUpperCase(), null, null,
                    null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                    fieldList.get(i).getDataTypeName(), false, false, false));
        }
    }

    @Override
    public void executeUpdate(String sql) throws Exception {
        client.queryWithPushDown(sql);
    }
}
