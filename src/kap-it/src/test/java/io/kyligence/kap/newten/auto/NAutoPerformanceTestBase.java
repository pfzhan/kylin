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
package io.kyligence.kap.newten.auto;

import com.google.common.collect.Lists;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

public class NAutoPerformanceTestBase extends NAutoTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(NAutoPerformanceTestBase.class);
    private static final String SOAK_SQL_DIR = "../kap-it/src/test/resources/query_for_soak_test";

    @Override
    String getFolder(String subFolder) {
        return SOAK_SQL_DIR + File.separator + subFolder;
    }

    public ProposeStats testWithBadQueries(int round, int factor) throws Exception {
        List<String> sqlList = collectQueries(
                new TestScenario("s_common"),
                new TestScenario("m_tpch"),
                new TestScenario("l_sinai"),
                new TestScenario("invalid"));
        return batchPropose(sqlList, factor, round);
    }

    public void testWithBadQueriesOneByOne(int round) throws Exception {
        List<String> sqlList = collectQueries(
                new TestScenario("s_common"),
                new TestScenario("m_tpch"),
                new TestScenario("l_sinai"),
                new TestScenario("invalid"));
        for (int i = 0; i < round; i++) {
            proposeOneByOne(sqlList);
        }
    }

    public ProposeStats batchPropose(List<String> sqlList, int factor, int round) {
        ProposeStats proposeStats = new ProposeStats();
        String[] sqls = generateMoreSqls(sqlList.toArray(new String[0]), factor);
        int numOfQueries = sqls.length;
        Runtime runtime = Runtime.getRuntime();
        NumberFormat format = NumberFormat.getInstance();


        long proposeStartTime = System.currentTimeMillis();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();
        long proposeEndTime = System.currentTimeMillis();

        double proposeTotalTime = getTotalTime(proposeStartTime, proposeEndTime);
        int qps = (int) (numOfQueries / proposeTotalTime);

        logger.info("The propose time of round {} is {}s", round + 1, proposeTotalTime);
        logger.info("----------------------------------------------");
        logger.info("The average QPS is {}", qps);

        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        int successAccelerateCount = 0;

        for (String sql : sqls) {
            // For simple query like "select 1 from table", will not be written in accelerateInfoMap
            if (accelerateInfoMap.get(sql) == null || !accelerateInfoMap.get(sql).isBlocked()) {
                successAccelerateCount++;
            }
        }

        logger.info("The size of accelerate info map is {}", accelerateInfoMap.size());
        double percent = 100f * successAccelerateCount / numOfQueries;
        Double successProposeRate = roundToScale(percent, 2);
        logger.info("The success propose rate is {}%", successProposeRate);

        // write propose stats
        proposeStats.setQps(qps);
        proposeStats.setSuccessProposeRate(successProposeRate);
        proposeStats.setNumOfQueries(numOfQueries);
        proposeStats.setRound(round + 1);

        // print memory usage info
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        logger.info("Free memory: {} kb", format.format(freeMemory / 1024));
        logger.info("Allocated memory: {} kb", format.format(allocatedMemory / 1024));
        logger.info("Max memory: {} kb", format.format(maxMemory / 1024));
        logger.info("Total free memory: {} kb", format.format((freeMemory + maxMemory - allocatedMemory) / 1024));
        logger.info("----------------------------------------------");

        return proposeStats;
    }

    public void proposeOneByOne(List<String> sqlList) {
        String[] sqls = sqlList.toArray(new String[0]);

        double minTime = Double.MAX_VALUE;
        double maxTime = 0;
        double totalTime = 0;
        int numOfQueries = sqls.length;

        for (int i = 0; i < numOfQueries; i++) {
            long proposeStartTime = System.currentTimeMillis();

            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[]{sqls[i]});
            smartMaster.runAll();

            long proposeEndTime = System.currentTimeMillis();
            double proposeTime = getTotalTime(proposeStartTime, proposeEndTime);
            totalTime += proposeTime;

            // update maxTime and minTime
            minTime = proposeTime < minTime ? proposeTime : minTime;
            maxTime = proposeTime > maxTime ? proposeTime : maxTime;
            logger.info("The QPS of round {} is: {}", i + 1, roundToScale(1 / proposeTime, 2));
        }

        logger.info("The matrix of {} SQL statements are shown below: ", numOfQueries);
        logger.info("--------------------------------------------------");
        logger.info("The maximum propose time is: {}s", maxTime);
        logger.info("The minimum propose time is: {}s", minTime);
        logger.info("The maximum QPS is {}", roundToScale(1 / minTime, 2));
        logger.info("The minimum QPS is {}", roundToScale(1 / maxTime, 2));
        logger.info("The average QPS is {}", roundToScale(numOfQueries / totalTime, 2));
    }

    private String[] generateMoreSqls(String[] sqls, int factor) {
        if (factor < 1) {
            return sqls;
        }
        List<String> sqlList = Lists.newArrayList();

        for (int i = 0; i < sqls.length; i++) {
            // Remove limit n first, then append new "limit n"s for each query
            String currentSql = sqls[i].replaceAll("limit\\s+\\d+", "");
            for (int j = 1; j <= factor; j++) {
                String generatedSql = currentSql + ("\nLIMIT " + j);
                sqlList.add(generatedSql);
            }
        }
        String[] sqlArr = new String[sqlList.size()];
        return sqlList.toArray(sqlArr);
    }

    private Double roundToScale(Double val, int scale) {
        return new BigDecimal(val.toString()).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

    protected double getTotalTime(long startTime, long endTime) {
        return 1.0 * (endTime - startTime) / 1000;
    }

    public void printProposeSummary(Map<Integer, ProposeStats> proposeSummary, int n) {
        logger.info("Below is the stats of {} queries in {} rounds proposal", proposeSummary.get(1).getNumOfQueries(),
                n);
        logger.info("----------------------------------------------");
        int sumOfQps = 0;
        for (Map.Entry<Integer, ProposeStats> entry : proposeSummary.entrySet()) {
            ProposeStats proposeStats = entry.getValue();
            int qps = proposeStats.getQps();
            sumOfQps += qps;
            logger.info("The average QPS of round {} is {}", entry.getKey(), qps);
            logger.info("The success propose rate is {}%", proposeStats.getSuccessProposeRate());
        }
        logger.info("The average QPS of {}-rounds proposal is {}", n, sumOfQps / n);
    }

    @Getter
    @Setter
    public static class ProposeStats {
        private int round;
        private Double successProposeRate;
        private int qps;
        private int numOfQueries;
    }
}
