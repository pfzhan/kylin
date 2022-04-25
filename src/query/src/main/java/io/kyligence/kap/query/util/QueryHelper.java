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

package io.kyligence.kap.query.util;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.engine.QueryExec;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.util.QueryParams;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;


public class QueryHelper {

    private QueryHelper() {
    }

    static final String RUN_CONSTANT_QUERY_LOCALLY = "kylin.query.engine.run-constant-query-locally";

    // Expose this method for MDX, don't remove it.
    public static Dataset<Row> singleQuery(String sql, String project) throws SQLException {
        val prevRunLocalConf = Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, "FALSE");
        try {
            val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getConfig();
            val queryExec = new QueryExec(project, projectKylinConfig);
            val queryParams = new QueryParams(KapQueryUtil.getKylinConfig(project),
                    sql, project, 0, 0, queryExec.getDefaultSchemaName(), true);
            val convertedSql = KapQueryUtil.massageSql(queryParams);
            queryExec.executeQuery(convertedSql);
        } finally {
            if (prevRunLocalConf == null) {
                Unsafe.clearProperty(RUN_CONSTANT_QUERY_LOCALLY);
            } else {
                Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, prevRunLocalConf);
            }
        }
        return SparderEnv.getDF();
    }

    public static Dataset<Row> sql(SparkSession session, String project, String sqlText) {
        try {
            return singleQuery(sqlText, project);
        } catch (SQLException e) {
            return session.sql(sqlText);
        }
    }
}
