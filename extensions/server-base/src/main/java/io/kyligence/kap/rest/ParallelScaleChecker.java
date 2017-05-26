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

package io.kyligence.kap.rest;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.rest.client.KAPRESTClient;
import io.kyligence.kap.storage.hbase.HBaseUtil;

class ParallelScaleChecker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ParallelScaleChecker.class);
    private static final int SPARK_EXECUTOR_EST_NUM_PER_NODE = 5;

    private int parallelLimit = -1;
    private KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    ParallelScaleChecker(final int parallelLimit) {
        this.parallelLimit = parallelLimit;
    }

    @Override
    public void run() {
        if (System.getProperty("kap.version").contains("Plus")) {
            checkInKapPlus();
        } else { // check hbase region servers number in KAP.
            checkInKap();
        }
    }

    private void checkInKap() {
        int queryNodeNum = kylinConfig.getRestServers().length;
        int hbaseRegionServerNum = HBaseUtil.getRegionServerNum();

        if (hbaseRegionServerNum + queryNodeNum > parallelLimit) {
            warning(queryNodeNum, hbaseRegionServerNum);
        }
    }

    private void checkInKapPlus() {
        String[] servers = kylinConfig.getRestServers();
        int queryNodeNum = servers.length;
        double sparkExecNum = 0;
        for (String server : servers) {
            KAPRESTClient restClient = new KAPRESTClient(server, null);
            sparkExecNum += restClient.retrieveSparkExecutorNum();
        }

        int sparkNodeEstNum = (int) Math.ceil(sparkExecNum / SPARK_EXECUTOR_EST_NUM_PER_NODE);
        if (sparkNodeEstNum + queryNodeNum > parallelLimit) {
            warning(queryNodeNum, sparkNodeEstNum);
        }
    }

    private void warning(int queryParallel, int storageParallel) {
        logger.warn("Parallel Scale exceeds the threshold: Used={}, Threshold={}", queryParallel + storageParallel, parallelLimit);
    }
}
