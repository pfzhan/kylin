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

package io.kyligence.kap.common.metric;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;
import lombok.var;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Joiner;

public class RDBMSWriter implements MetricWriter {
    public static final String QUERY_ID = "query_id";
    public static final String PROJECT_NAME = "project_name";
    public static final String SQL_TEXT = "sql_text";
    public static final String SQL_PATTERN = "sql_pattern";
    public static final String QUERY_DURATION = "duration";
    public static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
    public static final String TOTAL_SCAN_COUNT = "total_scan_count";
    public static final String RESULT_ROW_COUNT = "result_row_count";
    public static final String SUBMITTER = "submitter";
    public static final String REALIZATIONS = "realizations";
    public static final String QUERY_SERVER = "server";
    public static final String ERROR_TYPE = "error_type";
    public static final String ENGINE_TYPE = "engine_type";
    public static final String IS_CACHE_HIT = "cache_hit";
    public static final String QUERY_STATUS = "query_status";
    public static final String IS_INDEX_HIT = "index_hit";
    public static final String QUERY_TIME = "query_time";
    public static final String MONTH = "month";
    public static final String QUERY_FIRST_DAY_OF_MONTH = "query_first_day_of_month";
    public static final String QUERY_FIRST_DAY_OF_WEEK = "query_first_day_of_week";
    public static final String QUERY_DAY = "query_day";
    public static final String IS_TABLE_INDEX_USED = "is_table_index_used";
    public static final String IS_AGG_INDEX_USED = "is_agg_index_used";
    public static final String IS_TABLE_SNAPSHOT_USED = "is_table_snapshot_used";
    public static final String RESERVED_FIELD_3 = "reserved_field_3";

    public static final String MODEL = "model";
    public static final String LAYOUT_ID = "layout_id";
    public static final String INDEX_TYPE = "index_type";

    public static final String VARCHAR = " varchar(255)";
    public static final String BIGINT = " bigint";
    public static final String BOOLEAN = " boolean";

    // table names
    public static final String QUERY_MEASUREMENT_SURFIX = "query_history";
    public static final String REALIZATION_MEASUREMENT_SURFIX = "query_history_realization";

    public static final String INSERT_HISTORY_SQL = "INSERT INTO %s ("
            + Joiner.on(",").join(QUERY_ID, SQL_TEXT, SQL_PATTERN, QUERY_DURATION, TOTAL_SCAN_BYTES, TOTAL_SCAN_COUNT,
                    RESULT_ROW_COUNT, SUBMITTER, REALIZATIONS, QUERY_SERVER, ERROR_TYPE, ENGINE_TYPE, IS_CACHE_HIT,
                    QUERY_STATUS, IS_INDEX_HIT, QUERY_TIME, MONTH, QUERY_FIRST_DAY_OF_MONTH, QUERY_FIRST_DAY_OF_WEEK,
                    QUERY_DAY, IS_TABLE_INDEX_USED, IS_AGG_INDEX_USED, IS_TABLE_SNAPSHOT_USED, PROJECT_NAME, RESERVED_FIELD_3)
            + ")  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public static final String INSERT_HISTORY_REALIZATION_SQL = "INSERT INTO %s ("
            + Joiner.on(",").join(MODEL, LAYOUT_ID, INDEX_TYPE, QUERY_ID, QUERY_DURATION, QUERY_TIME, PROJECT_NAME)
            + ") VALUES (?,?,?,?,?,?,?)";

    volatile JdbcTemplate jdbcTemplate;

    private static volatile RDBMSWriter INSTANCE;

    public static RDBMSWriter getInstance() throws Exception {
        if (INSTANCE == null) {
            synchronized (RDBMSWriter.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new RDBMSWriter(KylinConfig.getInstanceFromEnv());
            }
        }

        return INSTANCE;
    }

    private RDBMSWriter(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        jdbcTemplate = new JdbcTemplate(dataSource);
        createQueryHistoryIfNotExist(kylinConfig);
        createQueryHistoryRealizationIfNotExist(kylinConfig);
    }

    @Override
    public void write(String measurement, QueryMetrics metrics, long timestamp) {
        // write to query history
        jdbcTemplate.update(String.format(INSERT_HISTORY_SQL, getQueryHistoryTableName()), metrics.getQueryId(), metrics.getSql(),
                metrics.getSqlPattern(), metrics.getQueryDuration(), metrics.getTotalScanBytes(),
                metrics.getTotalScanCount(), metrics.getResultRowCount(), metrics.getSubmitter(),
                metrics.getRealizations(), metrics.getServer(), metrics.getErrorType(), metrics.getEngineType(),
                metrics.isCacheHit(), metrics.getQueryStatus(), metrics.isIndexHit(), metrics.getQueryTime(),
                metrics.getMonth(), metrics.getQueryFirstDayOfMonth(), metrics.getQueryFirstDayOfWeek(),
                metrics.getQueryDay(), metrics.isTableIndexUsed(), metrics.isAggIndexUsed(), metrics.isTableIndexUsed(),
                metrics.getProjectName(), metrics.getRecordInfo().getBytes());

        // write to query history realization
        List<QueryMetrics.RealizationMetrics> realizationMetrics = metrics.getRealizationMetrics();
        for (QueryMetrics.RealizationMetrics realizationMetric : realizationMetrics) {
            jdbcTemplate.update(String.format(INSERT_HISTORY_REALIZATION_SQL, getQueryHistoryRealizationTableName()),
                    realizationMetric.getModelId(), realizationMetric.getLayoutId(), realizationMetric.getIndexType(),
                    realizationMetric.getQueryId(), realizationMetric.getDuration(), realizationMetric.getQueryTime(),
                    realizationMetric.getProjectName());
        }
    }

    @Override
    public void batchWrite(String measurement, List<QueryMetrics> metricsList, long timestamp) {
        // write to query history
        List<Object[]> queryHistoryList = Lists.newArrayList();
        for (QueryMetrics queryMetrics : metricsList) {
            queryHistoryList.add(new Object[] { queryMetrics.getQueryId(), queryMetrics.getSql(),
                    queryMetrics.getSqlPattern(), queryMetrics.getQueryDuration(), queryMetrics.getTotalScanBytes(),
                    queryMetrics.getTotalScanCount(), queryMetrics.getResultRowCount(), queryMetrics.getSubmitter(),
                    queryMetrics.getRealizations(), queryMetrics.getServer(), queryMetrics.getErrorType(),
                    queryMetrics.getEngineType(), queryMetrics.isCacheHit(), queryMetrics.getQueryStatus(),
                    queryMetrics.isIndexHit(), queryMetrics.getQueryTime(), queryMetrics.getMonth(),
                    queryMetrics.getQueryFirstDayOfMonth(), queryMetrics.getQueryFirstDayOfWeek(),
                    queryMetrics.getQueryDay(), queryMetrics.isTableIndexUsed(), queryMetrics.isAggIndexUsed(),
                    queryMetrics.isTableIndexUsed(), queryMetrics.getProjectName(), queryMetrics.getRecordInfo().getBytes() });
        }
        jdbcTemplate.batchUpdate(String.format(INSERT_HISTORY_SQL, getQueryHistoryTableName()), queryHistoryList);

        // write to query history realization
        List<Object[]> queryHistoryRealizationList = Lists.newArrayList();
        for (QueryMetrics queryMetrics : metricsList) {
            List<QueryMetrics.RealizationMetrics> realizationMetrics = queryMetrics.getRealizationMetrics();
            for (QueryMetrics.RealizationMetrics realizationMetric : realizationMetrics) {
                queryHistoryRealizationList.add(new Object[] { realizationMetric.getModelId(),
                        realizationMetric.getLayoutId(), realizationMetric.getIndexType(),
                        realizationMetric.getQueryId(), realizationMetric.getDuration(),
                        realizationMetric.getQueryTime(), realizationMetric.getProjectName() });
            }
        }
        jdbcTemplate.batchUpdate(String.format(INSERT_HISTORY_REALIZATION_SQL, getQueryHistoryRealizationTableName()),
                queryHistoryRealizationList);
    }


    void createQueryHistoryIfNotExist(KylinConfig kylinConfig) throws Exception {
        String metadataIdentifier = StorageURL.replaceUrl(kylinConfig.getMetadataUrl());
        String tableName = metadataIdentifier + "_" + QUERY_MEASUREMENT_SURFIX;

        if (JdbcUtil.isTableExists(jdbcTemplate.getDataSource().getConnection(), tableName)) {
            return;
        }

        Properties properties = getProperties();
        var createQueryHistorSql = properties.getProperty("create.queryhistory.store.table");
        jdbcTemplate.execute(String.format(createQueryHistorSql, tableName));

        createIndexForTable(properties, tableName, "create.queryhistory.store.tableindex1");
        createIndexForTable(properties, tableName, "create.queryhistory.store.tableindex2");
        createIndexForTable(properties, tableName, "create.queryhistory.store.tableindex3");
        createIndexForTable(properties, tableName, "create.queryhistory.store.tableindex4");
        createIndexForTable(properties, tableName, "create.queryhistory.store.tableindex5");
    }

    void createQueryHistoryRealizationIfNotExist(KylinConfig kylinConfig) throws Exception {
        String metadataIdentifier = StorageURL.replaceUrl(kylinConfig.getMetadataUrl());
        String tableName = metadataIdentifier + "_" + REALIZATION_MEASUREMENT_SURFIX;
        if (JdbcUtil.isTableExists(jdbcTemplate.getDataSource().getConnection(), tableName)) {
            return;
        }
        Properties properties = getProperties();
        var queryHistorRealizationSql = properties.getProperty("create.queryhistoryrealization.store.table");
        jdbcTemplate.execute(String.format(queryHistorRealizationSql, tableName));

        createIndexForTable(properties, tableName, "create.queryhistoryrealization.store.tableindex1");
        createIndexForTable(properties, tableName, "create.queryhistoryrealization.store.tableindex2");
    }
    
    void createIndexForTable(Properties properties, String tableName, String indexProperty) {
        var createQueryHistorIndexSql = properties.getProperty(indexProperty);
        jdbcTemplate.execute(String.format(createQueryHistorIndexSql, tableName, tableName));
    }

    private Properties getProperties() throws Exception {
        String fileName = "metadata-jdbc-default.properties";
        if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.postgresql.Driver")) {
            fileName = "metadata-jdbc-postgresql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName()
                .equals("com.mysql.jdbc.Driver")) {
            fileName = "metadata-jdbc-mysql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.h2.Driver")){
            fileName = "metadata-jdbc-h2.properties";
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

    public static String getQueryHistoryTableName() {
        String metadataIdentifier = StorageURL.replaceUrl(KylinConfig.getInstanceFromEnv().getMetadataUrl());
        return metadataIdentifier + "_" + QUERY_MEASUREMENT_SURFIX;
    }

    public static String getQueryHistoryRealizationTableName() {
        String metadataIdentifier = StorageURL.replaceUrl(KylinConfig.getInstanceFromEnv().getMetadataUrl());
        return metadataIdentifier + "_" + REALIZATION_MEASUREMENT_SURFIX;
    }

    @Override
    public String getType() {
        return Type.RDBMS.name();
    }
}
