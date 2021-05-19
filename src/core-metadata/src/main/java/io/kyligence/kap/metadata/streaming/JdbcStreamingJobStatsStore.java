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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.logging.LogOutputStream;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.streaming.util.StreamingJobStatsStoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.max;
import static org.mybatis.dynamic.sql.SqlBuilder.min;
import static org.mybatis.dynamic.sql.SqlBuilder.select;
import static org.mybatis.dynamic.sql.SqlBuilder.sum;

@Slf4j
public class JdbcStreamingJobStatsStore {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String TOTAL_ROW_COUNT = "count";
    public static final String MIN_RATE = "minRate";
    public static final String MAX_RATE = "maxRate";

    private final StreamingJobStatsTable streamingJobStatsTable;


    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String tableName;

    public JdbcStreamingJobStatsStore(KylinConfig config) throws Exception {
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        tableName = StorageURL.replaceUrl(url) + "_" + StreamingJobStats.STREAMING_JOB_STATS_SURFIX;
        streamingJobStatsTable = new StreamingJobStatsTable(tableName);
        sqlSessionFactory = StreamingJobStatsStoreUtil.getSqlSessionFactory(dataSource, tableName);
    }

    public void dropTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(String.format(Locale.ROOT, "drop table %s;", tableName).getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
        }
    }

    public int insert(StreamingJobStats streamingJobStats) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobStatsMapper sjsMapper = session.getMapper(StreamingJobStatsMapper.class);
            InsertStatementProvider<StreamingJobStats> insertStatement = getInsertSJSProvider(streamingJobStats);
            int rows = sjsMapper.insert(insertStatement);
            if (rows > 0) {
                log.debug("Insert one streaming job stats(job id:{}, time:{}) into database.", streamingJobStats.getJobId(), streamingJobStats.getCreateTime());
            }
            session.commit();
            return rows;
        }
    }

    public void insert(List<StreamingJobStats> statsList) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            StreamingJobStatsMapper mapper = session.getMapper(StreamingJobStatsMapper.class);
            List<InsertStatementProvider<StreamingJobStats>> providers = Lists.newArrayList();
            statsList.forEach(stats -> providers.add(getInsertSJSProvider(stats)));
            providers.forEach(mapper::insert);
            session.commit();
            if (statsList.size() > 0) {
                log.info("Insert {} streaming job stats into database takes {} ms", statsList.size(),
                        System.currentTimeMillis() - startTime);
            }
        }
    }

    public StreamingStatistics queryStreamingStatistics(long startTime, String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingStatisticsMapper mapper = session.getMapper(StreamingStatisticsMapper.class);
            SelectStatementProvider statementProvider = queryAllTimeAvgConsumerRate(startTime, jobId);
            return mapper.selectOne(statementProvider);
        }
    }

    public List<RowCountDetailByTime> queryRowCountDetailByTime(long startTime, String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RowCountDetailByTimeMapper mapper = session.getMapper(RowCountDetailByTimeMapper.class);
            SelectStatementProvider statementProvider = queryAvgConsumerRateByTime(jobId, startTime);
            return mapper.selectMany(statementProvider);
        }
    }

    public void deleteStreamingJobStats() {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobStatsMapper mapper = session.getMapper(StreamingJobStatsMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(streamingJobStatsTable) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            if (deleteRows > 0) {
                log.info("Delete {} row streaming job stats takes {} ms", deleteRows, System.currentTimeMillis() - startTime);
            }
        }
    }

    public void deleteStreamingJobStats(long timeline) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobStatsMapper mapper = session.getMapper(StreamingJobStatsMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(streamingJobStatsTable) //
                    .where(streamingJobStatsTable.createTime, isLessThan(timeline)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            if (deleteRows > 0) {
                log.info("Delete {} row streaming job stats takes {} ms", deleteRows, System.currentTimeMillis() - startTime);
            }
        }
    }


    InsertStatementProvider<StreamingJobStats> getInsertSJSProvider(StreamingJobStats stats) {
        return SqlBuilder.insert(stats).into(streamingJobStatsTable)
                .map(streamingJobStatsTable.jobId).toPropertyWhenPresent("jobId", stats::getJobId) //
                .map(streamingJobStatsTable.projectName).toPropertyWhenPresent("projectName", stats::getProjectName) //
                .map(streamingJobStatsTable.batchRowNum).toPropertyWhenPresent("batchRowNum", stats::getBatchRowNum) //
                .map(streamingJobStatsTable.rowsPerSecond).toPropertyWhenPresent("rowsPerSecond", stats::getRowsPerSecond) //
                .map(streamingJobStatsTable.createTime).toPropertyWhenPresent("createTime", stats::getCreateTime) //
                .map(streamingJobStatsTable.durationMs).toPropertyWhenPresent("durationMs", stats::getDurationMs) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryAllTimeAvgConsumerRate(long startTime, String jobId) {
        return select(min(streamingJobStatsTable.rowsPerSecond).as(MIN_RATE),
                max(streamingJobStatsTable.rowsPerSecond).as(MAX_RATE),
                sum(streamingJobStatsTable.batchRowNum).as(TOTAL_ROW_COUNT))
                .from(streamingJobStatsTable)
                .where(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(startTime))
                .and(streamingJobStatsTable.jobId, isEqualTo(jobId))
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryAvgConsumerRateByTime(String jobId, long startTime) {
        return select(getSelectFields(streamingJobStatsTable))
                .from(streamingJobStatsTable)
                .where(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(getLastHourRetainTime()))
                .and(streamingJobStatsTable.jobId, isEqualTo(jobId))
                .and(streamingJobStatsTable.batchRowNum, isGreaterThan(0L))
                .and(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(startTime))
                .orderBy(streamingJobStatsTable.createTime.descending())
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private long getLastHourRetainTime() {
        return new Date(System.currentTimeMillis() - 60 * 60 * 1000).getTime();
    }

    private BasicColumn[] getSelectFields(StreamingJobStatsTable streamingJobStatsTable) {
        return BasicColumn.columnList(streamingJobStatsTable.createTime, streamingJobStatsTable.batchRowNum);
    }
}
