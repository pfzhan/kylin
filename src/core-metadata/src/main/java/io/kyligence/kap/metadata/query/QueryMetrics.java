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
package io.kyligence.kap.metadata.query;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.common.scheduler.SchedulerEventNotifier;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryMetrics extends SchedulerEventNotifier {

    protected static final Logger logger = LoggerFactory.getLogger(QueryMetrics.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    public static final String UNKNOWN = "Unknown";

    public static final String AGG_INDEX = "Agg Index";
    public static final String TABLE_INDEX = "Table Index";
    public static final String TABLE_SNAPSHOT = "Table Snapshot";
    public static final String TOTAL_SCAN_COUNT = "totalScanCount";
    public static final String TOTAL_SCAN_BYTES = "totalScanBytes";

    // fields below are columns in InfluxDB table which records down query history
    protected long id;
    protected final String queryId;
    protected long queryTime;
    protected String projectName;

    protected String sql;
    protected String sqlPattern;

    protected String submitter;
    protected String server;

    protected long queryDuration;
    protected long totalScanBytes;
    protected long totalScanCount;
    protected long resultRowCount;
    protected long queryJobCount;
    protected long queryStageCount;
    protected long queryTaskCount;

    protected boolean isPushdown;
    protected String engineType;

    protected boolean isCacheHit;
    protected String cacheType;
    protected String queryMsg;
    protected boolean isIndexHit;
    protected boolean isTimeout;

    protected String errorType;
    protected String queryStatus;

    protected String month;
    protected long queryFirstDayOfMonth;
    protected long queryFirstDayOfWeek;
    protected long queryDay;

    protected boolean tableIndexUsed;
    protected boolean aggIndexUsed;
    protected boolean tableSnapshotUsed;

    protected String defaultServer;

    protected QueryHistoryInfo queryHistoryInfo;

    public QueryMetrics(String queryId) {
        this.queryId = queryId;
    }

    public QueryMetrics(String queryId, String defaultServer) {
        this.queryId = queryId;
        this.defaultServer = defaultServer;
    }

    public List<RealizationMetrics> getRealizationMetrics() {
        return ImmutableList.copyOf(queryHistoryInfo.realizationMetrics);
    }

    public boolean isSucceed() {
        return QueryHistory.QUERY_HISTORY_SUCCEEDED.equals(queryStatus);
    }

    public boolean isSecondStorage() {
        for (RealizationMetrics metrics: getRealizationMetrics()) {
            if (metrics.isSecondStorage)
                return true;
        }
        return false;
    }

    @Getter
    @Setter
    // fields in this class are columns in InfluxDB table which records down query history's realization info
    public static class RealizationMetrics implements Serializable {

        protected String queryId;

        protected long duration;

        protected String layoutId;

        protected String indexType;

        protected String modelId;

        protected long queryTime;

        protected String projectName;

        protected boolean isSecondStorage;

        protected boolean isStreamingLayout;

        protected List<String> snapshots;

        // For serialize
        public RealizationMetrics() {}

        public RealizationMetrics(String layoutId, String indexType, String modelId, List<String> snapshots) {
            this.layoutId = layoutId;
            this.indexType = indexType;
            this.modelId = modelId;
            this.snapshots = snapshots;
        }
    }
}
