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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.adhoc;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PushDownRunnerSparkImpl implements IPushDownRunner {
    public static final Logger logger = LoggerFactory.getLogger(PushDownRunnerSparkImpl.class);

    @Override
    public void init(KylinConfig config) {
        // SparkSession has been initialized
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas) {

        PushdownResponse response = queryWithPushDown(query);

        int columnCount = response.getColumns().size();
        List<StructField> fieldList = response.getColumns();

        results.addAll(response.getRows());

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).isNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true, Integer.MAX_VALUE,
                    fieldList.get(i).getName().toUpperCase(), fieldList.get(i).getName().toUpperCase(), null, null,
                    null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                    fieldList.get(i).getDataTypeName(), false, false, false));
        }
    }

    @Override
    public void executeUpdate(String sql) {
        queryWithPushDown(sql);
    }

    private PushdownResponse queryWithPushDown(String sql) {
        return SparkSubmitter.submitPushDownTask(sql);
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_HIVE;
    }
}
