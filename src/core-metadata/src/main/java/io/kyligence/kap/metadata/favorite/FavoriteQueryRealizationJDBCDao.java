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

package io.kyligence.kap.metadata.favorite;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

public class FavoriteQueryRealizationJDBCDao implements FavoriteQueryRealizationDao {

    private final static String TABLE_PREFIX = "favorite_query_realization_";
    private String tableName;
    private KylinConfig config;

    private static final String SQL_PATTERN_HASH = "sql_pattern_hash";
    private static final String MODEL_ID = "model_id";
    private static final String SEMANTIC_VERSION = "semantic_version";

    private static final String CUBE_PLAN_ID = "cube_plan_id";
    private static final String CUBOID_LAYOUT_ID = "cuboid_layout_id";
    private static final String AND = " and ";

    public static FavoriteQueryRealizationJDBCDao getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, FavoriteQueryRealizationJDBCDao.class);
    }

    static FavoriteQueryRealizationJDBCDao newInstance(KylinConfig kylinConfig, String project) {
        return new FavoriteQueryRealizationJDBCDao(kylinConfig, project);
    }

    private FavoriteQueryRealizationJDBCDao(KylinConfig kylinConfig, String project) {
        this.tableName = TABLE_PREFIX + project;
        this.config = kylinConfig;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE IF NOT EXISTS %s", tableName));
        // columns
        sb.append(String.format(
                "(id INT UNSIGNED AUTO_INCREMENT, %s INT NOT NULL, %s VARCHAR(255) NOT NULL, %s INT NOT NULL, %s VARCHAR(255) NOT NULL, %s BIGINT, ",
                SQL_PATTERN_HASH, MODEL_ID, SEMANTIC_VERSION, CUBE_PLAN_ID, CUBOID_LAYOUT_ID));
        // primary key and indices
        sb.append(String.format(
                "PRIMARY KEY (id), INDEX %s_sql_pattern_hash_key (%s), INDEX %s_model_cube_index (%s ,%s ,%s))",
                this.tableName, SQL_PATTERN_HASH, this.tableName, MODEL_ID, CUBE_PLAN_ID, CUBOID_LAYOUT_ID));
        JDBCManager.getInstance(config).getJdbcTemplate().execute(sb.toString());
    }

    @Override
    public List<FavoriteQueryRealization> getByConditions(String modelName, String cubePlanName, Long cuboidLayoutId) {
        return getByConditions(modelName, cubePlanName, cuboidLayoutId, null);
    }

    private List<FavoriteQueryRealization> getByConditions(String modelName, String cubePlanName, Long cuboidLayoutId,
            Integer sqlPatternHash) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("SELECT * FROM %s ", this.tableName));

        String filterConditionStr = genFilterCondition(modelName, cubePlanName, cuboidLayoutId, sqlPatternHash);
        if (StringUtils.isNotBlank(filterConditionStr)) {
            sql.append(filterConditionStr);
        }
        return JDBCManager.getInstance(config).getJdbcTemplate().query(sql.toString(),
                new FavoriteQueryRealizationRowMapper());
    }

    @Override
    public List<FavoriteQueryRealization> getBySqlPatternHash(int sqlPatternHash) {
        return getByConditions(null, null, null, sqlPatternHash);
    }

    private String genFilterCondition(String modelName, String cubePlanName, Long cuboidLayoutId,
            Integer sqlPatternHash) {
        StringBuilder filterSql = new StringBuilder();
        if (StringUtils.isNotBlank(modelName)) {
            appendPrefix(filterSql);
            filterSql.append(String.format(" %s = '%s'", MODEL_ID, modelName));
        }
        if (StringUtils.isNotBlank(cubePlanName)) {
            appendPrefix(filterSql);
            filterSql.append(String.format(" %s = '%s'", CUBE_PLAN_ID, cubePlanName));
        }
        if (null != cuboidLayoutId) {
            appendPrefix(filterSql);
            filterSql.append(String.format(" %s = '%d'", CUBOID_LAYOUT_ID, cuboidLayoutId));
        }
        if (null != sqlPatternHash) {
            appendPrefix(filterSql);
            filterSql.append(String.format(" %s = '%d'", SQL_PATTERN_HASH, sqlPatternHash));
        }

        if (filterSql.length() > 0) {
            filterSql.insert(0, " WHERE ");
        }

        return filterSql.toString();
    }

    private void appendPrefix(StringBuilder filterSql) {
        if (filterSql.length() > 0) {
            filterSql.append(AND);
        }
    }

    @Override
    public void batchInsert(List<FavoriteQueryRealization> favoriteQueryRealizations) {
        if (CollectionUtils.isEmpty(favoriteQueryRealizations))
            return;

        JDBCManager.getInstance(KylinConfig.getInstanceFromEnv()).getTransactionTemplate()
                .execute(new TransactionCallbackWithoutResult() {
                    @Override
                    public void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
                        try {
                            innerInsert(favoriteQueryRealizations);
                        } catch (Exception e) {
                            transactionStatus.setRollbackOnly();
                            throw e;
                        }
                    }
                });
    }

    @Override
    public void batchDelete(List<FavoriteQueryRealization> favoriteQueryRealizations) {
        if (CollectionUtils.isEmpty(favoriteQueryRealizations))
            return;

        JDBCManager.getInstance(KylinConfig.getInstanceFromEnv()).getTransactionTemplate()
                .execute(new TransactionCallbackWithoutResult() {
                    @Override
                    public void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
                        try {
                            innerDelete(favoriteQueryRealizations);
                        } catch (Exception e) {
                            transactionStatus.setRollbackOnly();
                            throw e;
                        }
                    }
                });
    }

    private void innerDelete(List<FavoriteQueryRealization> favoriteQueryRealizations) {
        String sql = String.format("DELETE FROM %s WHERE %s=?", this.tableName, SQL_PATTERN_HASH);

        JDBCManager.getInstance(KylinConfig.getInstanceFromEnv()).getJdbcTemplate().batchUpdate(sql,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        FavoriteQueryRealization favoriteQueryRealization = favoriteQueryRealizations.get(i);
                        preparedStatement.setInt(1, favoriteQueryRealization.getSqlPatternHash());
                    }

                    @Override
                    public int getBatchSize() {
                        return favoriteQueryRealizations.size();
                    }
                });
    }

    private void innerInsert(List<FavoriteQueryRealization> favoriteQueryRealizations) {
        String sql = String.format(
                "INSERT INTO %s (sql_pattern_hash, model_id, semantic_version, cube_plan_id, cuboid_layout_id) VALUES(?, ?, ?, ?, ?) ",
                this.tableName);

        JDBCManager.getInstance(KylinConfig.getInstanceFromEnv()).getJdbcTemplate().batchUpdate(sql,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        FavoriteQueryRealization favoriteQueryRealization = favoriteQueryRealizations.get(i);
                        preparedStatement.setInt(1, favoriteQueryRealization.getSqlPatternHash());
                        preparedStatement.setString(2, favoriteQueryRealization.getModelId());
                        preparedStatement.setInt(3, favoriteQueryRealization.getSemanticVersion());
                        preparedStatement.setString(4, favoriteQueryRealization.getCubePlanId());
                        preparedStatement.setLong(5, favoriteQueryRealization.getCuboidLayoutId());
                    }

                    @Override
                    public int getBatchSize() {
                        return favoriteQueryRealizations.size();
                    }
                });
    }

    public class FavoriteQueryRealizationRowMapper implements RowMapper<FavoriteQueryRealization> {

        @Override
        public FavoriteQueryRealization mapRow(ResultSet resultSet, int i) throws SQLException {
            FavoriteQueryRealization favoriteQueryRealization = new FavoriteQueryRealization();
            favoriteQueryRealization.setSqlPatternHash(resultSet.getInt(SQL_PATTERN_HASH));
            favoriteQueryRealization.setModelId(resultSet.getString(MODEL_ID));
            favoriteQueryRealization.setSemanticVersion(resultSet.getInt(SEMANTIC_VERSION));
            favoriteQueryRealization.setCubePlanId(resultSet.getString(CUBE_PLAN_ID));
            favoriteQueryRealization.setCuboidLayoutId(resultSet.getLong(CUBOID_LAYOUT_ID));
            return favoriteQueryRealization;
        }
    }
}
