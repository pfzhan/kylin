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
package io.kyligence.kap.common.persistence.metadata;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.base.Joiner;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcEpochStore extends EpochStore {

    static final String EPOCH_ID = "epoch_id";
    static final String EPOCH_TARGET = "epoch_target";
    static final String CURRENT_EPOCH_OWNER = "current_epoch_owner";
    static final String LAST_EPOCH_RENEW_TIME = "last_epoch_renew_time";
    static final String SERVER_MODE = "server_mode";
    static final String MAINTENANCE_MODE_REASON = "maintenance_mode_reason";
    static final String MVCC = "mvcc";

    static final String INSERT_SQL = "insert into %s (" + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET,
            CURRENT_EPOCH_OWNER, LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC)
            + ") values (?, ?, ?, ?, ?, ?, ?)";

    static final String SELECT_SQL = "select " + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET, CURRENT_EPOCH_OWNER,
            LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC) + " from %s";

    static final String UPDATE_SQL = "update %s set " + EPOCH_ID + " =?, " + CURRENT_EPOCH_OWNER + " =?, "
            + LAST_EPOCH_RENEW_TIME + " =?, " + SERVER_MODE + " =?, " + MAINTENANCE_MODE_REASON + " =?, " + MVCC
            + " =? where " + EPOCH_TARGET + " =? and " + MVCC + " =?";

    static final String DELETE_SQL = "delete from %s where " + EPOCH_TARGET + " =?";

    static final String SELECT_BY_EPOCH_TARGET_SQL = "select " + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET,
            CURRENT_EPOCH_OWNER, LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC) + " from %s where "
            + EPOCH_TARGET + " = '%s'";

    @Getter
    private static JdbcTemplate jdbcTemplate;
    @Getter
    private static String table;
    @Getter
    private static DataSourceTransactionManager transactionManager;

    public static EpochStore getEpochStore(KylinConfig config) {
        return Singletons.getInstance(JdbcEpochStore.class, (clz) -> new JdbcEpochStore(config));
    }

    private JdbcEpochStore(KylinConfig config) throws Exception {
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier() + EPOCH_SUFFIX;
    }

    public void createIfNotExist() throws Exception {
        if (isTableExists(jdbcTemplate.getDataSource().getConnection(), table)) {
            return;
        }
        String fileName = "metadata-jdbc-default.properties";
        if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.postgresql.Driver")) {
            fileName = "metadata-jdbc-postgresql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName()
                .equals("com.mysql.jdbc.Driver")) {
            fileName = "metadata-jdbc-mysql.properties";
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        var sql = properties.getProperty("create.epoch.store.table");

        withTransaction(transactionManager, () -> {
            jdbcTemplate.execute(
                    String.format(sql, table, EPOCH_ID, EPOCH_TARGET, CURRENT_EPOCH_OWNER, LAST_EPOCH_RENEW_TIME,
                            SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC, table, EPOCH_TARGET, EPOCH_TARGET));
            return 1;
        });
    }

    @Override
    public void saveOrUpdate(Epoch epoch) {
        withTransaction(transactionManager, () -> {
            if (getEpoch(epoch.getEpochTarget()) != null) {
                return jdbcTemplate.update(String.format(UPDATE_SQL, table), ps -> {
                    ps.setLong(1, epoch.getEpochId());
                    ps.setString(2, epoch.getCurrentEpochOwner());
                    ps.setLong(3, epoch.getLastEpochRenewTime());
                    ps.setString(4, epoch.getServerMode());
                    ps.setString(5, epoch.getMaintenanceModeReason());
                    ps.setLong(6, epoch.getMvcc() + 1);
                    ps.setString(7, epoch.getEpochTarget());
                    ps.setLong(8, epoch.getMvcc());
                });
            } else {
                return jdbcTemplate.update(String.format(INSERT_SQL, table), epoch.getEpochId(), epoch.getEpochTarget(),
                        epoch.getCurrentEpochOwner(), epoch.getLastEpochRenewTime(), epoch.getServerMode(),
                        epoch.getMaintenanceModeReason(), epoch.getMvcc());
            }
        });
    }

    @Override
    public Epoch getEpoch(String epochTarget) {
        return withTransaction(transactionManager, () -> {
            List<Epoch> result = jdbcTemplate.query(String.format(SELECT_BY_EPOCH_TARGET_SQL, table, epochTarget),
                    new EpochRowMapper());
            if (result.isEmpty()) {
                return null;
            }
            return result.get(0);
        });
    }

    @Override
    public List<Epoch> list() {
        return withTransaction(transactionManager,
                () -> jdbcTemplate.query(String.format(SELECT_SQL, table), new EpochRowMapper()));
    }

    @Override
    public void delete(String epochTarget) {
        withTransaction(transactionManager, () -> jdbcTemplate.update(String.format(DELETE_SQL, table), epochTarget));
    }
}
