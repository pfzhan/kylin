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

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.storage.adhoc.AdHocRunnerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.sql.Types;


public class AdHocRunnerSparkImpl extends AdHocRunnerBase {
    public static final Logger logger = LoggerFactory.getLogger(AdHocRunnerSparkImpl.class);

    private SparkDriverClient client;

    @Override
    public void init() {
        try {
            client = new SparkDriverClient(KapConfig.getInstanceFromEnv());
        } catch (Exception e) {
            logger.error("error is " + e.getLocalizedMessage());
            throw e;
        }
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws Exception {
        SparkJobProtos.AdHocResponse response = client.queryWithAdHoc((query));
        int columnCount = response.getColumnsCount();
        List<SparkJobProtos.StructField> fieldList = response.getColumnsList();

        for(SparkJobProtos.Row row: response.getRowsList()) {
            results.add(row.getDataList());
        }

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).getNullable() ? 1 : 0;
            int type = Types.VARCHAR;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, false, Integer.MAX_VALUE, "column" + i, fieldList.get(i).getName(), null, null, null, Integer.MAX_VALUE, 128, type, fieldList.get(i).getDataType(), false, false, false));
        }
    }
}
