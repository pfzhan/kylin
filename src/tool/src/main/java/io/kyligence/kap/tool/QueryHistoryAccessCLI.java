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

package io.kyligence.kap.tool;

import com.google.common.collect.Maps;
import io.kyligence.kap.common.metric.RDBMSWriter;
import lombok.val;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

public class QueryHistoryAccessCLI {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryAccessCLI.class);
    private static final String PROJECT = "test_project";

    public boolean testAccessQueryHistory() {
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String metadataIdentifier = StorageURL.replaceUrl(kylinConfig.getMetadataUrl());
            RDBMSWriter rdbmsWriter = RDBMSWriter.getInstance();

            HashMap<String, Object> queryHistoryHashMap = Maps.newHashMap();
            queryHistoryHashMap.put(RDBMSWriter.PROJECT_NAME, PROJECT);
            String queryHistoryTableName = metadataIdentifier + "_" + RDBMSWriter.QUERY_MEASUREMENT_SURFIX;
            rdbmsWriter.writeToQueryHistory("", queryHistoryTableName, queryHistoryHashMap);

            HashMap<String, Object> queryHistoryRealizationHashMap = Maps.newHashMap();
            queryHistoryRealizationHashMap.put(RDBMSWriter.PROJECT_NAME, PROJECT);
            String queryHistoryRealizationTableName = metadataIdentifier + "_"
                    + RDBMSWriter.REALIZATION_MEASUREMENT_SURFIX;
            rdbmsWriter.writeToQueryHistoryRealization("", queryHistoryRealizationTableName,
                    queryHistoryRealizationHashMap);

            // clean 
            getJdbcTemplate()
                    .update("delete from " + queryHistoryTableName + " where project_name = '" + PROJECT + "'");
            getJdbcTemplate().update(
                    "delete from " + queryHistoryRealizationTableName + " where project_name = '" + PROJECT + "'");
        } catch (Exception e) {
            logger.error("query history access test failed." + e.getMessage());
            return false;
        }
        return true;
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val props = datasourceParameters(kylinConfig.getMetadataUrl());
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    public static void main(String[] args) {
        QueryHistoryAccessCLI cli = new QueryHistoryAccessCLI();

        if (args.length != 1) {
            System.exit(1);
        }

        long repetition = Long.parseLong(args[0]);

        while (repetition > 0) {
            if (false == cli.testAccessQueryHistory()) {
                logger.error("Test failed.");
                System.exit(1);
            }
            repetition--;
        }

        logger.info("Test succeed.");
        System.exit(0);
    }
}
