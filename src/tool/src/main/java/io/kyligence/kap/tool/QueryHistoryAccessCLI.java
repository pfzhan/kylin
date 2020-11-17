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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;

public class QueryHistoryAccessCLI {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryAccessCLI.class);
    private static final String PROJECT = "test_project";
    private static final String FAIL_LOG = "query history access test failed.";
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    public QueryHistoryAccessCLI() {
        this.queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
    }

    public boolean testAccessQueryHistory() {
        try {
            QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
            queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
            queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
            queryMetrics.setQueryDuration(5578L);
            queryMetrics.setTotalScanBytes(863L);
            queryMetrics.setTotalScanCount(4096L);
            queryMetrics.setResultRowCount(500L);
            queryMetrics.setSubmitter("ADMIN");
            queryMetrics.setRealizations("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
            queryMetrics.setErrorType("");
            queryMetrics.setCacheHit(true);
            queryMetrics.setIndexHit(true);
            queryMetrics.setQueryTime(1584888338274L);
            queryMetrics.setProjectName(PROJECT);
            QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);
            queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

            QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                    "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
            realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
            realizationMetrics.setDuration(4591L);
            realizationMetrics.setQueryTime(1586405449387L);
            realizationMetrics.setProjectName(PROJECT);

            List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
            realizationMetricsList.add(realizationMetrics);
            realizationMetricsList.add(realizationMetrics);
            queryMetrics.setRealizationMetrics(realizationMetricsList);
            queryHistoryDAO.insert(queryMetrics);

            // clean test data
            queryHistoryDAO.deleteQueryHistoryByProject(PROJECT);
            queryHistoryDAO.deleteAllQueryHistoryRealizationForProject(PROJECT);
        } catch (Exception e) {
            logger.error(FAIL_LOG, e);
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        QueryHistoryAccessCLI cli = null;
        try {
            cli = new QueryHistoryAccessCLI();
        } catch (Exception e) {
            logger.error("Test failed.", e);
            System.exit(1);
        }

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
