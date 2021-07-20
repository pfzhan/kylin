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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.adaptor.AdaptorConfig;
import org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

// for reflection
public class DefaultSourceConnector extends DefaultAdaptor implements ISourceConnector {

    public DefaultSourceConnector(AdaptorConfig config) throws Exception {
        super(config);
    }

    // Reflected by JdbcSourceInput#getSourceData only, do not abuse it!
    public DefaultSourceConnector(){
        super();
    }

    @Override
    public Dataset<Row> getSourceData(KylinConfig kylinConfig, SparkSession sparkSession, String sql,
            Map<String, String> params) {
        String url = kylinConfig.getJdbcConnectionUrl();
        String user = kylinConfig.getJdbcUser();
        String password = kylinConfig.getJdbcPass();
        String driver = kylinConfig.getJdbcDriver();
        return sparkSession.read().format("jdbc").option("url", url).option("user", user).option("password", password)
                .option("driver", driver).option("query", sql).options(params).load();
    }

}