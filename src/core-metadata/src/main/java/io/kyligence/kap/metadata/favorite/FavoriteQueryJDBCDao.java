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

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FavoriteQueryJDBCDao implements FavoriteQueryDao {

    private String tableName;
    private KylinConfig config;

    private static final String SQL_PATTERN_HASH = "sql_pattern_hash";
    private static final String PROJECT = "project";
    private static final String SQL_PATTERN = "sql_pattern";
    private static final String LAST_QUERY_TIME = "last_query_time";
    private static final String TOTAL_COUNT = "total_count";
    private static final String SUCCESS_COUNT = "success_count";
    private static final String SUCCESS_RATE = "success_rate";
    private static final String TOTAL_DURATION = "total_duration";
    private static final String AVERAGE_DURATION = "average_duration";
    private static final String STATUS = "status";

    public static Map<String, Set<Integer>> getSqlPatternHashSet() {
        return sqlPatternHashSet;
    }

    public static void setSqlPatternHashSet(Map<String, Set<Integer>> updatedMap) {
        sqlPatternHashSet = updatedMap;
    }

    private static Map<String, Set<Integer>> sqlPatternHashSet;

    public static FavoriteQueryJDBCDao getInstance(KylinConfig kylinConfig) {
        return kylinConfig.getManager(FavoriteQueryJDBCDao.class);
    }

    static FavoriteQueryJDBCDao newInstance(KylinConfig kylinConfig) {
        return new FavoriteQueryJDBCDao(kylinConfig);
    }

    private FavoriteQueryJDBCDao(KylinConfig kylinConfig) {
        this.config = kylinConfig;
        this.tableName = kylinConfig.getFavoriteStorageUrl().getIdentifier();
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE IF NOT EXISTS %s", this.tableName));
        // columns
        sb.append(String.format(
                "(id INT UNSIGNED AUTO_INCREMENT, %s INT NOT NULL, %s VARCHAR(255) NOT NULL, %s TEXT NOT NULL, %s BIGINT, %s INT, %s INT, %s DECIMAL(10, 6), %s BIGINT, %s DECIMAL(18, 6), %s ENUM('%s', '%s', '%s', '%s') DEFAULT '%s', ",
                SQL_PATTERN_HASH, PROJECT, SQL_PATTERN, LAST_QUERY_TIME, TOTAL_COUNT, SUCCESS_COUNT, SUCCESS_RATE,
                TOTAL_DURATION, AVERAGE_DURATION, STATUS, FavoriteQueryStatusEnum.WAITING,
                FavoriteQueryStatusEnum.ACCELERATING, FavoriteQueryStatusEnum.PARTLY_ACCELERATED,
                FavoriteQueryStatusEnum.FULLY_ACCELERATED, FavoriteQueryStatusEnum.WAITING));
        // primary key and indices
        sb.append(String.format(
                "PRIMARY KEY (id), INDEX sql_pattern_hash_index (%s), INDEX project_index (%s), INDEX last_query_time_index (%s), INDEX status_index (%s))",
                SQL_PATTERN_HASH, PROJECT, LAST_QUERY_TIME, STATUS));
        JDBCManager.getInstance(config).getJdbcTemplate().execute(sb.toString());
    }

    public synchronized void initializeSqlPatternSet() {
        //todo: batch query
        final String sql = String.format("SELECT sql_pattern_hash, project FROM %s", this.tableName);
        List<Pair<String, Integer>> queryResults = JDBCManager.getInstance(config).getJdbcTemplate().query(sql, new RowMapper<Pair<String, Integer>>() {
            @Override
            public Pair<String, Integer> mapRow(ResultSet resultSet, int i) throws SQLException {
                Pair<String, Integer> row = new Pair<>();
                row.setFirst(resultSet.getString(PROJECT));
                row.setSecond(resultSet.getInt(SQL_PATTERN_HASH));
                return row;
            }
        });

        sqlPatternHashSet = Maps.newHashMap();

        for (Pair<String, Integer> oneRow : queryResults) {
            String project = oneRow.getFirst();
            final Integer sqlPatternHash = oneRow.getSecond();
            Set<Integer> sqlPatternHashSetInProj = sqlPatternHashSet.get(project);
            if (sqlPatternHashSetInProj == null) {
                sqlPatternHashSetInProj = new HashSet<>();
                sqlPatternHashSetInProj.add(sqlPatternHash);
            } else {
                sqlPatternHashSetInProj.add(sqlPatternHash);
            }

            sqlPatternHashSet.put(project, sqlPatternHashSetInProj);
        }
    }

    public static boolean isInDatabase(String project, int sqlPatternHash) {
        Set<Integer> sqlPatternSetInProj = sqlPatternHashSet.get(project);

        if (sqlPatternSetInProj == null)
            return false;

        return sqlPatternSetInProj.contains(sqlPatternHash);
    }

    // only for test
    public void dropTable() {
        final String sql = "DROP TABLE IF EXISTS " + this.tableName;
        JDBCManager.getInstance(config).getJdbcTemplate().execute(sql);
    }

    // todo: transaction
    @Override
    public void batchUpdate(final List<FavoriteQuery> favoriteQueries) {
        if (favoriteQueries == null || favoriteQueries.isEmpty())
            return;

        JDBCManager.getInstance(config).getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
                try {
                    innerUpdate(favoriteQueries);
                } catch (Exception e) {
                    transactionStatus.setRollbackOnly();
                    throw e;
                }
            }
        });
    }

    private void innerUpdate(final List<FavoriteQuery> favoriteQueries) {
        final String updateSql = String.format(
                "UPDATE %s SET last_query_time=?, total_count=total_count+?, success_count=success_count+?, "
                        + "success_rate=success_count/total_count, total_duration=total_duration+?, average_duration=total_duration/total_count WHERE %s=? and %s=?",
                this.tableName, SQL_PATTERN_HASH, PROJECT);

        JDBCManager.getInstance(config).getJdbcTemplate().batchUpdate(updateSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                FavoriteQuery favoriteQuery = favoriteQueries.get(i);
                preparedStatement.setLong(1, favoriteQuery.getLastQueryTime());
                preparedStatement.setInt(2, favoriteQuery.getTotalCount());
                preparedStatement.setLong(3, favoriteQuery.getSuccessCount());
                preparedStatement.setLong(4, favoriteQuery.getTotalDuration());
                preparedStatement.setInt(5, favoriteQuery.getSqlPatternHash());
                preparedStatement.setString(6, favoriteQuery.getProject());
            }

            @Override
            public int getBatchSize() {
                return favoriteQueries.size();
            }
        });
    }

    // todo: transaction
    @Override
    public void batchUpdateStatus(final List<FavoriteQuery> favoriteQueries) {
        if (favoriteQueries == null || favoriteQueries.isEmpty())
            return;

        JDBCManager.getInstance(config).getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
                try {
                    innerUpdateStatus(favoriteQueries);
                } catch (Exception e) {
                    transactionStatus.setRollbackOnly();
                    throw e;
                }
            }
        });
    }

    private void innerUpdateStatus(final List<FavoriteQuery> favoriteQueries) {
        final String updateSql = String.format("UPDATE %s SET %s=? WHERE %s=? and %s=?", this.tableName, STATUS,
                SQL_PATTERN_HASH, PROJECT);

        JDBCManager.getInstance(config).getJdbcTemplate().batchUpdate(updateSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                FavoriteQuery favoriteQuery = favoriteQueries.get(i);
                preparedStatement.setString(1, favoriteQuery.getStatus().toString());
                preparedStatement.setInt(2, favoriteQuery.getSqlPatternHash());
                preparedStatement.setString(3, favoriteQuery.getProject());
            }

            @Override
            public int getBatchSize() {
                return favoriteQueries.size();
            }
        });
    }

    @Override
    public List<FavoriteQueryResponse> getByPage(String project, int limit, int offset) {
        final String sql = String.format("SELECT * FROM %s WHERE project='%s' ORDER BY %s DESC LIMIT %d OFFSET %d",
                this.tableName, project, LAST_QUERY_TIME, limit, offset*limit);
        return JDBCManager.getInstance(config).getJdbcTemplate().query(sql, new FavoriteRowMapper());
    }

    @Override
    public List<String> getUnAcceleratedSqlPattern(String project) {
        final String sql = String.format("SELECT %s FROM %s WHERE project='%s' AND status='%s'", SQL_PATTERN,
                this.tableName, project, FavoriteQueryStatusEnum.WAITING);
        return JDBCManager.getInstance(config).getJdbcTemplate().query(sql, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSet.getString(SQL_PATTERN);
            }
        });
    }

    public FavoriteQuery getFavoriteQuery(int sqlPatternHash, String project) {
        final String sql = String.format("SELECT * FROM %s WHERE project='%s' AND sql_pattern_hash = %d",
                this.tableName, project, sqlPatternHash);
        try {
            return JDBCManager.getInstance(config).getJdbcTemplate().queryForObject(sql, new FavoriteRowMapper());
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    // todo: transaction
    @Override
    public void batchInsert(final List<FavoriteQuery> favoriteQueries) {
        if (favoriteQueries == null || favoriteQueries.isEmpty())
            return;

        JDBCManager.getInstance(config).getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
                try {
                    innerInsert(favoriteQueries);
                } catch (Exception e) {
                    transactionStatus.setRollbackOnly();
                    throw e;
                }
            }
        });
    }

    private void innerInsert(final List<FavoriteQuery> favoriteQueries) {
        String sql = String.format(
                "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                this.tableName, SQL_PATTERN_HASH, PROJECT, SQL_PATTERN, LAST_QUERY_TIME, TOTAL_COUNT, SUCCESS_COUNT,
                SUCCESS_RATE, TOTAL_DURATION, AVERAGE_DURATION, STATUS);

        JDBCManager.getInstance(config).getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                FavoriteQuery favoriteQuery = favoriteQueries.get(i);
                preparedStatement.setInt(1, favoriteQuery.getSqlPatternHash());
                preparedStatement.setString(2, favoriteQuery.getProject());
                preparedStatement.setString(3, favoriteQuery.getSqlPattern());
                preparedStatement.setLong(4, favoriteQuery.getLastQueryTime());
                preparedStatement.setInt(5, favoriteQuery.getTotalCount());
                preparedStatement.setInt(6, favoriteQuery.getSuccessCount());
                if (favoriteQuery.getTotalCount() != 0) {
                    preparedStatement.setFloat(7, favoriteQuery.getSuccessCount() / (float) favoriteQuery.getTotalCount());
                    preparedStatement.setFloat(9, favoriteQuery.getTotalDuration() / (float) favoriteQuery.getTotalCount());
                } else {
                    preparedStatement.setFloat(7, 0);
                    preparedStatement.setFloat(9, 0);
                }
                preparedStatement.setLong(8, favoriteQuery.getTotalDuration());
                preparedStatement.setString(10, favoriteQuery.getStatus().toString());
            }

            @Override
            public int getBatchSize() {
                return favoriteQueries.size();
            }
        });
    }

    public class FavoriteRowMapper implements RowMapper<FavoriteQueryResponse> {

        @Override
        public FavoriteQueryResponse mapRow(ResultSet resultSet, int i) throws SQLException {
            FavoriteQueryResponse favoriteQuery = new FavoriteQueryResponse();
            favoriteQuery.setSqlPattern(resultSet.getString(SQL_PATTERN));
            favoriteQuery.setSqlPatternHash(resultSet.getInt(SQL_PATTERN_HASH));
            favoriteQuery.setProject(resultSet.getString(PROJECT));
            favoriteQuery.setLastQueryTime(resultSet.getLong(LAST_QUERY_TIME));
            favoriteQuery.setTotalCount(resultSet.getInt(TOTAL_COUNT));
            favoriteQuery.setSuccessCount(resultSet.getInt(SUCCESS_COUNT));
            favoriteQuery.setSuccessRate(resultSet.getFloat(SUCCESS_RATE));
            favoriteQuery.setTotalDuration(resultSet.getLong(TOTAL_DURATION));
            favoriteQuery.setAverageDuration(resultSet.getFloat(AVERAGE_DURATION));
            favoriteQuery.setStatus(FavoriteQueryStatusEnum.valueOf(resultSet.getString(STATUS)));

            return favoriteQuery;
        }
    }
}
