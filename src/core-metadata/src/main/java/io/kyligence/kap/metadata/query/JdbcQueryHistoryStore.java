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

import static org.mybatis.dynamic.sql.SqlBuilder.avg;
import static org.mybatis.dynamic.sql.SqlBuilder.count;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isLike;
import static org.mybatis.dynamic.sql.SqlBuilder.or;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.QueryExpressionDSL;
import org.mybatis.dynamic.sql.select.SelectModel;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.query.util.QueryHisStoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcQueryHistoryStore {

    public static final String MONTH = "month";
    public static final String WEEK = "week";
    public static final String DAY = "day";

    private final QueryHistoryTable queryHistoryTable;
    private final QueryHistoryRealizationTable queryHistoryRealizationTable;

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private DataSource dataSource;
    String qhTableName;
    String qhRealizationTableName;

    public JdbcQueryHistoryStore(KylinConfig config) throws Exception {
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        qhTableName = StorageURL.replaceUrl(url) + "_" + QueryHistory.QUERY_MEASUREMENT_SURFIX;
        qhRealizationTableName = StorageURL.replaceUrl(url) + "_" + QueryHistory.REALIZATION_MEASUREMENT_SURFIX;
        queryHistoryTable = new QueryHistoryTable(qhTableName);
        queryHistoryRealizationTable = new QueryHistoryRealizationTable(qhRealizationTableName);
        sqlSessionFactory = QueryHisStoreUtil.getSqlSessionFactory(dataSource, qhTableName, qhRealizationTableName);
    }

    public void dropQueryHistoryTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(String.format("drop table %s;", qhTableName).getBytes())));
        }
    }

    public int insert(QueryMetrics queryMetrics) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper qhMapper = session.getMapper(QueryHistoryMapper.class);
            InsertStatementProvider<QueryMetrics> insertStatement = getInsertQhProvider(queryMetrics);
            int rows = qhMapper.insert(insertStatement);

            QueryHistoryRealizationMapper qhRealizationMapper = session.getMapper(QueryHistoryRealizationMapper.class);
            List<InsertStatementProvider<QueryMetrics.RealizationMetrics>> insertQhRealProviderList = Lists
                    .newArrayList();
            queryMetrics.getRealizationMetrics().forEach(realizationMetrics -> insertQhRealProviderList
                    .add(getInsertQhRealizationProvider(realizationMetrics)));
            insertQhRealProviderList.forEach(qhRealizationMapper::insert);

            if (rows > 0) {
                log.debug("Insert one query history(query id:{}) into database.", queryMetrics.getQueryId());
            }
            session.commit();
            return rows;
        }
    }

    public void insert(List<QueryMetrics> queryMetricsList) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            List<InsertStatementProvider<QueryMetrics>> providers = Lists.newArrayList();
            queryMetricsList.forEach(queryMetrics -> providers.add(getInsertQhProvider(queryMetrics)));
            providers.forEach(mapper::insert);

            QueryHistoryRealizationMapper qhRealizationMapper = session.getMapper(QueryHistoryRealizationMapper.class);
            List<InsertStatementProvider<QueryMetrics.RealizationMetrics>> insertQhRealProviderList = Lists
                    .newArrayList();
            queryMetricsList.forEach(queryMetrics -> queryMetrics.getRealizationMetrics()
                    .forEach(realizationMetrics -> insertQhRealProviderList
                            .add(getInsertQhRealizationProvider(realizationMetrics))));
            insertQhRealProviderList.forEach(qhRealizationMapper::insert);

            session.commit();
            log.info("Insert {} query history into database takes {} ms", queryMetricsList.size(),
                    System.currentTimeMillis() - startTime);
        }
    }

    public List<QueryHistory> queryQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            SelectStatementProvider statementProvider = queryQueryHistoriesByConditionsProvider(request, limit, offset);
            return mapper.selectMany(statementProvider);
        }
    }

    public QueryStatistics queryQueryHistoriesSize(QueryHistoryRequest request) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = queryQueryHistoriesSizeProvider(request);
            return mapper.selectOne(statementProvider);
        }
    }

    public QueryHistory queryOldestQueryHistory(long maxSize) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(queryHistoryTable)). //
                    from(queryHistoryTable) //
                    .orderBy(queryHistoryTable.id.descending()) //
                    .limit(1) //
                    .offset(maxSize - 1) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectOne(statementProvider);
        }
    }

    public QueryHistory queryOldestQueryHistory(long maxSize, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(queryHistoryTable)) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.projectName, isEqualTo(project)) //
                    .orderBy(queryHistoryTable.id.descending()) //
                    .limit(1) //
                    .offset(maxSize - 1) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectOne(statementProvider);
        }
    }

    public List<QueryHistory> queryAllQueryHistories() {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(queryHistoryTable)) //
                    .from(queryHistoryTable) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryHistory> queryQueryHistoriesByIdOffset(long id, int batchSize, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(queryHistoryTable)) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.id, isGreaterThan(id)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .orderBy(queryHistoryTable.id) //
                    .limit(batchSize) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryStatistics> queryCountAndAvgDuration(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(count(queryHistoryTable.queryId),
                    avg(queryHistoryTable.duration).as("mean")) //
                            .from(queryHistoryTable) //
                            .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                            .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                            .and(queryHistoryTable.projectName, isEqualTo(project)) //
                            .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryStatistics> queryCountByModel(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(queryHistoryRealizationTable.model,
                    count(queryHistoryRealizationTable.queryId).as("count")) //
                            .from(queryHistoryRealizationTable) //
                            .where(queryHistoryRealizationTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                            .and(queryHistoryRealizationTable.queryTime, isLessThan(endTime)) //
                            .and(queryHistoryRealizationTable.projectName, isEqualTo(project)) //
                            .groupBy(queryHistoryRealizationTable.model) //
                            .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryStatistics> queryCountByTime(long startTime, long endTime, String timeDimension, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = queryCountByTimeProvider(startTime, endTime, timeDimension,
                    project);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryStatistics> queryAvgDurationByModel(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(queryHistoryRealizationTable.model,
                    avg(queryHistoryRealizationTable.duration).as("mean")) //
                            .from(queryHistoryRealizationTable) //
                            .where(queryHistoryRealizationTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                            .and(queryHistoryRealizationTable.queryTime, isLessThan(endTime)) //
                            .and(queryHistoryRealizationTable.projectName, isEqualTo(project)) //
                            .groupBy(queryHistoryRealizationTable.model) //
                            .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<QueryStatistics> queryAvgDurationByTime(long startTime, long endTime, String timeDimension,
            String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryStatisticsMapper mapper = session.getMapper(QueryStatisticsMapper.class);
            SelectStatementProvider statementProvider = queryAvgDurationByTimeProvider(startTime, endTime,
                    timeDimension, project);
            return mapper.selectMany(statementProvider);
        }
    }

    public void deleteQueryHistory() {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryTable) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history takes {} ms", deleteRows, System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistory(long queryTime) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isLessThan(queryTime)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history takes {} ms", deleteRows, System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistory(long queryTime, String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isLessThan(queryTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history for project [{}] takes {} ms", deleteRows, project,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistory(String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryTable) //
                    .where(queryHistoryTable.projectName, isEqualTo(project)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history for project [{}] takes {} ms", deleteRows, project,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistoryRealization() {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryRealizationTable) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history realization takes {} ms", deleteRows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistoryRealization(long queryTime) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryRealizationTable) //
                    .where(queryHistoryRealizationTable.queryTime, isLessThan(queryTime)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history realization takes {} ms", deleteRows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistoryRealization(long queryTime, String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryRealizationTable) //
                    .where(queryHistoryRealizationTable.queryTime, isLessThan(queryTime)) //
                    .and(queryHistoryRealizationTable.projectName, isEqualTo(project)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history realization takes {} ms", deleteRows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteQueryHistoryRealization(String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(queryHistoryRealizationTable) //
                    .where(queryHistoryRealizationTable.projectName, isEqualTo(project)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row query history realization takes {} ms", deleteRows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void updateQueryHistoryInfo(List<Pair<Long, QueryHistoryInfo>> idToQHInfoList) {
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            QueryHistoryMapper mapper = session.getMapper(QueryHistoryMapper.class);
            List<UpdateStatementProvider> providers = Lists.newArrayList();
            idToQHInfoList.forEach(pair -> providers.add(changeQHInfoProvider(pair.getFirst(), pair.getSecond())));
            providers.forEach(mapper::update);
            session.commit();
            log.info("Update {} query history info takes {} ms", idToQHInfoList.size(),
                    System.currentTimeMillis() - start);
        }
    }

    InsertStatementProvider<QueryMetrics> getInsertQhProvider(QueryMetrics queryMetrics) {
        return SqlBuilder.insert(queryMetrics).into(queryHistoryTable).map(queryHistoryTable.queryId)
                .toPropertyWhenPresent("queryId", queryMetrics::getQueryId) //
                .map(queryHistoryTable.sql).toPropertyWhenPresent("sql", queryMetrics::getSql) //
                .map(queryHistoryTable.sqlPattern).toPropertyWhenPresent("sqlPattern", queryMetrics::getSqlPattern) //
                .map(queryHistoryTable.duration).toPropertyWhenPresent("queryDuration", queryMetrics::getQueryDuration) //
                .map(queryHistoryTable.totalScanBytes)
                .toPropertyWhenPresent("totalScanBytes", queryMetrics::getTotalScanBytes) //
                .map(queryHistoryTable.totalScanCount)
                .toPropertyWhenPresent("totalScanCount", queryMetrics::getTotalScanCount) //
                .map(queryHistoryTable.resultRowCount)
                .toPropertyWhenPresent("resultRowCount", queryMetrics::getResultRowCount) //
                .map(queryHistoryTable.querySubmitter).toPropertyWhenPresent("submitter", queryMetrics::getSubmitter) //
                .map(queryHistoryTable.queryRealizations)
                .toPropertyWhenPresent("realizations", queryMetrics::getRealizations) //
                .map(queryHistoryTable.hostName).toPropertyWhenPresent("server", queryMetrics::getServer) //
                .map(queryHistoryTable.errorType).toPropertyWhenPresent("errorType", queryMetrics::getErrorType) //
                .map(queryHistoryTable.engineType).toPropertyWhenPresent("engineType", queryMetrics::getEngineType) //
                .map(queryHistoryTable.cacheHit).toPropertyWhenPresent("cacheHit", queryMetrics::isCacheHit) //
                .map(queryHistoryTable.queryStatus).toPropertyWhenPresent("queryStatus", queryMetrics::getQueryStatus) //
                .map(queryHistoryTable.indexHit).toPropertyWhenPresent("indexHit", queryMetrics::isIndexHit) //
                .map(queryHistoryTable.queryTime).toPropertyWhenPresent("queryTime", queryMetrics::getQueryTime) //
                .map(queryHistoryTable.month).toPropertyWhenPresent("month", queryMetrics::getMonth) //
                .map(queryHistoryTable.queryFirstDayOfMonth)
                .toPropertyWhenPresent("queryFirstDayOfMonth", queryMetrics::getQueryFirstDayOfMonth) //
                .map(queryHistoryTable.queryFirstDayOfWeek)
                .toPropertyWhenPresent("queryFirstDayOfWeek", queryMetrics::getQueryFirstDayOfWeek) //
                .map(queryHistoryTable.queryDay).toPropertyWhenPresent("queryDay", queryMetrics::getQueryDay) //
                .map(queryHistoryTable.projectName).toPropertyWhenPresent("projectName", queryMetrics::getProjectName) //
                .map(queryHistoryTable.queryHistoryInfo).toProperty("queryHistoryInfo") //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    InsertStatementProvider<QueryMetrics.RealizationMetrics> getInsertQhRealizationProvider(
            QueryMetrics.RealizationMetrics realizationMetrics) {
        return SqlBuilder.insert(realizationMetrics).into(queryHistoryRealizationTable)
                .map(queryHistoryRealizationTable.model)
                .toPropertyWhenPresent("modelId", realizationMetrics::getModelId)
                .map(queryHistoryRealizationTable.layoutId)
                .toPropertyWhenPresent("layoutId", realizationMetrics::getLayoutId)
                .map(queryHistoryRealizationTable.indexType)
                .toPropertyWhenPresent("indexType", realizationMetrics::getIndexType)
                .map(queryHistoryRealizationTable.queryId)
                .toPropertyWhenPresent("queryId", realizationMetrics::getQueryId)
                .map(queryHistoryRealizationTable.duration)
                .toPropertyWhenPresent("duration", realizationMetrics::getDuration)
                .map(queryHistoryRealizationTable.queryTime)
                .toPropertyWhenPresent("queryTime", realizationMetrics::getQueryTime)
                .map(queryHistoryRealizationTable.projectName)
                .toPropertyWhenPresent("projectName", realizationMetrics::getProjectName).build()
                .render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryQueryHistoriesByConditionsProvider(QueryHistoryRequest request, int limit,
            int offset) {
        return filterByConditions(select(getSelectFields(queryHistoryTable)).from(queryHistoryTable), request)
                .orderBy(queryHistoryTable.queryTime.descending()) //
                .limit(limit) //
                .offset(offset) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryQueryHistoriesSizeProvider(QueryHistoryRequest request) {
        return filterByConditions(select(count(queryHistoryTable.queryId).as("count")).from(queryHistoryTable), request)
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private QueryExpressionDSL<SelectModel>.QueryExpressionWhereBuilder filterByConditions(
            QueryExpressionDSL<SelectModel> selectSql, QueryHistoryRequest request) {
        QueryExpressionDSL<SelectModel>.QueryExpressionWhereBuilder filterSql = selectSql.where();

        if (StringUtils.isNotEmpty(request.getStartTimeFrom()) && StringUtils.isNotEmpty(request.getStartTimeTo())) {
            filterSql = filterSql
                    .and(queryHistoryTable.queryTime, isGreaterThan(Long.valueOf(request.getStartTimeFrom())))
                    .and(queryHistoryTable.queryTime, isLessThan(Long.valueOf(request.getStartTimeTo())));
        }

        if (StringUtils.isNotEmpty(request.getLatencyFrom()) && StringUtils.isNotEmpty(request.getLatencyTo())) {
            filterSql = filterSql
                    .and(queryHistoryTable.duration, isGreaterThan(Long.valueOf(request.getLatencyFrom()) * 1000L))
                    .and(queryHistoryTable.duration, isLessThan(Long.valueOf(request.getLatencyTo()) * 1000L))
                    .and(queryHistoryTable.queryStatus, isEqualTo("SUCCEEDED"));
        }

        if (StringUtils.isNotEmpty(request.getServer())) {
            filterSql = filterSql.and(queryHistoryTable.hostName, isEqualTo(request.getServer()));
        }

        if (StringUtils.isNotEmpty(request.getSql())) {
            filterSql = filterSql.and(queryHistoryTable.sql, isLike(request.getSql()),
                    or(queryHistoryTable.queryId, isLike(request.getSql())));
        }

        if (request.getRealizations() != null && !request.getRealizations().isEmpty()) {
            boolean pushdown = request.getRealizations().contains("pushdown");
            boolean model = request.getRealizations().contains("modelName");
            if (pushdown && !model) {
                filterSql = filterSql.and(queryHistoryTable.indexHit, isEqualTo(false));
            } else if (!pushdown && model) {
                filterSql = filterSql.and(queryHistoryTable.indexHit, isEqualTo(true));
            } else if (!pushdown && !model) {
                throw new IllegalArgumentException("Illegal realization type ");
            }
        }

        if (request.getQueryStatus() != null && request.getQueryStatus().size() == 1) {
            filterSql = filterSql.and(queryHistoryTable.queryStatus, isEqualTo(request.getQueryStatus().get(0)));
        }

        filterSql = filterSql.and(queryHistoryTable.projectName, isEqualTo(request.getProject()));

        if (!request.isAdmin()) {
            filterSql = filterSql.and(queryHistoryTable.querySubmitter, isEqualTo(request.getUsername()));
        }

        return filterSql;
    }

    private UpdateStatementProvider changeQHInfoProvider(long id, QueryHistoryInfo queryHistoryInfo) {
        return SqlBuilder.update(queryHistoryTable) //
                .set(queryHistoryTable.queryHistoryInfo) //
                .equalTo(queryHistoryInfo) //
                .where(queryHistoryTable.id, isEqualTo(id)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryCountByTimeProvider(long startTime, long endTime, String timeDimension,
            String project) {
        switch (timeDimension) {
        case MONTH:
            return select(queryHistoryTable.queryFirstDayOfMonth.as("time"), count(queryHistoryTable.id).as("count")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryFirstDayOfMonth) //
                    .build().render(RenderingStrategies.MYBATIS3);
        case WEEK:
            return select(queryHistoryTable.queryFirstDayOfWeek.as("time"), count(queryHistoryTable.id).as("count")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryFirstDayOfWeek) //
                    .build().render(RenderingStrategies.MYBATIS3);
        default:
            return select(queryHistoryTable.queryDay.as("time"), count(queryHistoryTable.id).as("count")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryDay) //
                    .build().render(RenderingStrategies.MYBATIS3);
        }
    }

    private SelectStatementProvider queryAvgDurationByTimeProvider(long startTime, long endTime, String timeDimension,
            String project) {
        switch (timeDimension) {
        case MONTH:
            return select(queryHistoryTable.queryFirstDayOfMonth.as("time"), avg(queryHistoryTable.duration).as("mean")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryFirstDayOfMonth) //
                    .build().render(RenderingStrategies.MYBATIS3);
        case WEEK:
            return select(queryHistoryTable.queryFirstDayOfWeek.as("time"), avg(queryHistoryTable.duration).as("mean")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryFirstDayOfWeek) //
                    .build().render(RenderingStrategies.MYBATIS3);
        default:
            return select(queryHistoryTable.queryDay.as("time"), avg(queryHistoryTable.duration).as("mean")) //
                    .from(queryHistoryTable) //
                    .where(queryHistoryTable.queryTime, isGreaterThanOrEqualTo(startTime)) //
                    .and(queryHistoryTable.queryTime, isLessThan(endTime)) //
                    .and(queryHistoryTable.projectName, isEqualTo(project)) //
                    .groupBy(queryHistoryTable.queryDay) //
                    .build().render(RenderingStrategies.MYBATIS3);
        }
    }

    private BasicColumn[] getSelectFields(QueryHistoryTable queryHistoryTable) {
        return BasicColumn.columnList(queryHistoryTable.id, queryHistoryTable.cacheHit, queryHistoryTable.duration,
                queryHistoryTable.engineType, queryHistoryTable.errorType, queryHistoryTable.hostName,
                queryHistoryTable.indexHit, queryHistoryTable.projectName, queryHistoryTable.queryHistoryInfo,
                queryHistoryTable.queryId, queryHistoryTable.queryRealizations, queryHistoryTable.queryStatus,
                queryHistoryTable.querySubmitter, queryHistoryTable.queryTime, queryHistoryTable.resultRowCount,
                queryHistoryTable.sql, queryHistoryTable.sqlPattern, queryHistoryTable.totalScanBytes,
                queryHistoryTable.totalScanCount);
    }

}
