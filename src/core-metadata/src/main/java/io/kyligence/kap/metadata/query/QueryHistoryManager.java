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

package io.kyligence.kap.metadata.query;

import com.google.common.collect.Lists;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import io.kyligence.kap.shaded.influxdb.org.influxdb.impl.InfluxDBResultMapper;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryHistoryManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryManager.class);
    private volatile InfluxDB influxDB;

    public static QueryHistoryManager getInstance(KylinConfig config) {
        return config.getManager(QueryHistoryManager.class);
    }

    static QueryHistoryManager newInstance(KylinConfig kylinConfig) {
        return new QueryHistoryManager(kylinConfig);
    }

    private KapConfig kapConfig;

    private QueryHistoryManager(KylinConfig config) {
        logger.info("Initializing QueryHistoryManager with config " + config);
        this.kapConfig = KapConfig.wrap(config);
    }

    public InfluxDB getInfluxDB() {
        if (influxDB == null) {
            synchronized (this) {
                if (influxDB != null) {
                    return this.influxDB;
                }

                this.influxDB = InfluxDBFactory.connect("http://" + kapConfig.influxdbAddress(),
                        kapConfig.influxdbUsername(), kapConfig.influxdbPassword());
            }
        }

        return this.influxDB;
    }

    public <T> List<T> getQueryHistoriesBySql(String query, Class clazz) {
        if (!getInfluxDB().databaseExists(QueryHistory.DB_NAME))
            return Lists.newArrayList();
        final QueryResult result = getInfluxDB().query(new Query(query, QueryHistory.DB_NAME));
        final InfluxDBResultMapper mapper = new InfluxDBResultMapper();

        return mapper.toPOJO(result, clazz);
    }
}
