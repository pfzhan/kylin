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
import lombok.Setter;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class RDBMSStreamingJobStatsDAO implements StreamingJobStatsDAO {
    private static final Logger logger = LoggerFactory.getLogger(RDBMSStreamingJobStatsDAO.class);
    @Setter
    private String sJSMetricMeasurement;

    private JdbcStreamingJobStatsStore jdbcSJSStore;

    public static RDBMSStreamingJobStatsDAO getInstance() {
        return Singletons.getInstance(RDBMSStreamingJobStatsDAO.class);
    }

    public RDBMSStreamingJobStatsDAO() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing RDBMSStreamingJobStatsDAO with KylinConfig Id: {} ", System.identityHashCode(config));
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.sJSMetricMeasurement = metadataIdentifier + "_" + StreamingJobStats.STREAMING_JOB_STATS_SURFIX;
        jdbcSJSStore = new JdbcStreamingJobStatsStore(config);
    }

    public String getSJSMetricMeasurement() {
        return sJSMetricMeasurement;
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
        jdbcSJSStore.deleteStreamingJobStats();
    }

    public void deleteSJSIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcSJSStore.deleteStreamingJobStats(retainTime);
    }

    public static long getRetainTime() {
        return new Date(
                System.currentTimeMillis() - KylinConfig.getInstanceFromEnv().getStreamingJobStatsSurvivalThreshold())
                        .getTime();
    }

    public List<RowCountDetailByTime> queryRowCountDetailByTime(long startTime, String jobId) {
        return jdbcSJSStore.queryRowCountDetailByTime(startTime, jobId);
    }

    public StreamingStatistics getStreamingStatistics(long startTime, String jobId) {
        return jdbcSJSStore.queryStreamingStatistics(startTime, jobId);
    }

}
