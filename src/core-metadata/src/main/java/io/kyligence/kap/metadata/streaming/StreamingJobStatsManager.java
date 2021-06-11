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

package io.kyligence.kap.metadata.streaming;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class StreamingJobStatsManager {
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobStatsManager.class);
    @Setter
    @Getter
    private String sJSMetricMeasurement;

    private JdbcStreamingJobStatsStore jdbcSJSStore;

    public static StreamingJobStatsManager getInstance() {
        return Singletons.getInstance(StreamingJobStatsManager.class);
    }

    public StreamingJobStatsManager() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing StreamingJobStatsManager with KylinConfig Id: {} ",
                    System.identityHashCode(config));
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.sJSMetricMeasurement = metadataIdentifier + "_" + StreamingJobStats.STREAMING_JOB_STATS_SUFFIX;
        jdbcSJSStore = new JdbcStreamingJobStatsStore(config);
    }

    public int insert(StreamingJobStats stats) {
        return jdbcSJSStore.insert(stats);
    }

    public void insert(List<StreamingJobStats> statsList) {
        jdbcSJSStore.insert(statsList);
    }

    public void dropTable() throws SQLException {
        jdbcSJSStore.dropTable();
    }

    public void deleteAllStreamingJobStats() {
        jdbcSJSStore.deleteStreamingJobStats(-1L);
    }

    public void deleteSJSIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcSJSStore.deleteStreamingJobStats(retainTime);
    }

    public static long getRetainTime() {
        return new Date(System.currentTimeMillis()
                - KylinConfig.getInstanceFromEnv().getStreamingJobStatsSurvivalThreshold() * 24 * 60 * 60 * 1000)
                        .getTime();
    }

    public List<RowCountDetailByTime> queryRowCountDetailByTime(long startTime, String jobId) {
        return jdbcSJSStore.queryRowCountDetailByTime(startTime, jobId);
    }

    public ConsumptionRateStats countAvgConsumptionRate(long startTime, String jobId) {
        return jdbcSJSStore.queryAvgConsumptionRate(startTime, jobId);
    }

    public List<StreamingJobStats> queryStreamingJobStats(long startTime, String jobId) {
        return jdbcSJSStore.queryByJobId(startTime, jobId);
    }

    public StreamingJobStats getLatestOneByJobId(String jobId) {
        return jdbcSJSStore.getLatestOneByJobId(jobId);
    }

    public Map<String, Long> queryDataLatenciesByJobIds(List<String> jobIds) {
        if (jobIds.isEmpty()) {
            return Collections.emptyMap();
        }
        return jdbcSJSStore.queryDataLatenciesByJobIds(jobIds);
    }
}
