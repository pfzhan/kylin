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
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isIndexExists;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CompressionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Joiner;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.metadata.jdbc.AuditLogRowMapper;
import io.kyligence.kap.common.persistence.transaction.AbstractAuditLogReplayWorker;
import io.kyligence.kap.common.persistence.transaction.AuditLogGroupedReplayWorker;
import io.kyligence.kap.common.persistence.transaction.AuditLogReplayWorker;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogStore implements AuditLogStore {

    static final String AUDIT_LOG_SUFFIX = "_audit_log";

    static final String AUDIT_LOG_TABLE_ID = "id";
    static final String AUDIT_LOG_TABLE_KEY = "meta_key";
    static final String AUDIT_LOG_TABLE_CONTENT = "meta_content";
    static final String AUDIT_LOG_TABLE_TS = "meta_ts";
    static final String AUDIT_LOG_TABLE_MVCC = "meta_mvcc";
    static final String AUDIT_LOG_TABLE_UNIT = "unit_id";
    static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    static final String AUDIT_LOG_TABLE_INSTANCE = "instance";
    static final String CREATE_TABLE = "create.auditlog.store.table";
    static final String META_KEY_META_MVCC_INDEX = "%s_meta_key_meta_mvcc_index";
    static final String META_KEY_META_MVCC_INDEX_KEY = "create.auditlog.store.tableindex.meta_key_meta_mvcc_index";

    static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + ") values (?, ?, ?, ?, ?, ?, ?)";
    static final String SELECT_BY_RANGE_SQL = "select "
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where id > %d and id <= %d order by id";

    static final String SELECT_BY_PROJECT_RANGE_SQL = "select "
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where meta_key like '/%s/%%' and id > %d and id <= %d order by id";

    static final String SELECT_MAX_ID_BY_PROJECT_SQL = "select max(id) from %s where id > %d and meta_key like '/%s/%%'";
    static final String SELECT_MAX_ID_SQL = "select max(id) from %s";
    static final String SELECT_MIN_ID_SQL = "select min(id) from %s";
    static final String SELECT_COUNT_ID_RANGE = "select count(id) from %s where id >= %d and id < %d";
    static final String DELETE_ID_LESSTHAN_SQL = "delete from %s where id < ?";
    static final String SELECT_TS_RANGE = "select "
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where id < %d and meta_ts between %d and %d order by id desc limit %d";

    static final String SELECT_BY_META_KET_AND_MVCC = "select "
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where meta_key = '%s' and meta_mvcc = %s";

    private final KylinConfig config;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    @Getter
    private final String table;

    private final AbstractAuditLogReplayWorker replayWorker;

    private String instance;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    public JdbcAuditLogStore(KylinConfig config) throws Exception {
        this(config, -1);
    }

    public JdbcAuditLogStore(KylinConfig config, int timeout) throws Exception {
        this.config = config;
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setQueryTimeout(timeout);
        table = url.getIdentifier() + AUDIT_LOG_SUFFIX;

        instance = AddressUtil.getLocalInstance();
        createIfNotExist();
        replayWorker = config.auditLogGroupByProjectReload() ? new AuditLogGroupedReplayWorker(config, this)
                : new AuditLogReplayWorker(config, this);
    }

    public JdbcAuditLogStore(KylinConfig config, JdbcTemplate jdbcTemplate,
            DataSourceTransactionManager transactionManager, String table) throws Exception {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionManager = transactionManager;
        this.table = table;
        instance = AddressUtil.getLocalInstance();

        createIfNotExist();
        replayWorker = config.auditLogGroupByProjectReload() ? new AuditLogGroupedReplayWorker(config, this)
                : new AuditLogReplayWorker(config, this);

    }


    public void save(UnitMessages unitMessages) {
        val unitId = unitMessages.getUnitId();
        val operator = Optional.ofNullable(SecurityContextHolder.getContext().getAuthentication())
                .map(Principal::getName).orElse(null);
        withTransaction(transactionManager,
                () -> jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table),
                        unitMessages.getMessages().stream().map(e -> {
                            if (e instanceof ResourceCreateOrUpdateEvent) {
                                ResourceCreateOrUpdateEvent createEvent = (ResourceCreateOrUpdateEvent) e;
                                try {
                                    return new Object[] { createEvent.getResPath(),
                                            CompressionUtils
                                                    .compress(createEvent.getCreatedOrUpdated().getByteSource().read()),
                                            createEvent.getCreatedOrUpdated().getTimestamp(),
                                            createEvent.getCreatedOrUpdated().getMvcc(), unitId, operator, instance };
                                } catch (IOException ignore) {
                                    return null;
                                }
                            } else if (e instanceof ResourceDeleteEvent) {
                                ResourceDeleteEvent deleteEvent = (ResourceDeleteEvent) e;
                                return new Object[] { deleteEvent.getResPath(), null, System.currentTimeMillis(), null,
                                        unitId, operator, instance };
                            }
                            return null;
                        }).filter(Objects::nonNull).collect(Collectors.toList())));
    }

    public void batchInsert(List<AuditLog> auditLogs) {
        withTransaction(transactionManager, () -> jdbcTemplate
                .batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table), auditLogs.stream().map(x -> {
                    try {
                        val bs = Objects.isNull(x.getByteSource()) ? null : x.getByteSource().read();
                        return new Object[] { x.getResPath(), CompressionUtils.compress(bs), x.getTimestamp(),
                                x.getMvcc(), x.getUnitId(), x.getOperator(), x.getInstance() };
                    } catch (IOException e) {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList())));
    }

    public List<AuditLog> fetch(long currentId, long size) {
        log.trace("fetch log from {} < id <= {}", currentId, currentId + size);
        return jdbcTemplate.query(String.format(Locale.ROOT, SELECT_BY_RANGE_SQL, table, currentId, currentId + size),
                new AuditLogRowMapper());
    }

    public List<AuditLog> fetch(String project, long currentId, long size) {
        log.trace("fetch log from {} < id <= {}", currentId, currentId + size);
        return jdbcTemplate.query(
                String.format(Locale.ROOT, SELECT_BY_PROJECT_RANGE_SQL, table, project, currentId, currentId + size),
                new AuditLogRowMapper());
    }

    public List<AuditLog> fetchRange(long fromId, long start, long end, int limit) {
        log.trace("Fetch log from {} meta_ts between {} and {}, fromId: {}.", table, start, end, fromId);
        return jdbcTemplate.query(String.format(Locale.ROOT, SELECT_TS_RANGE, table, fromId, start, end, limit),
                new AuditLogRowMapper());
    }

    @Override
    public long getMaxId() {
        return Optional
                .ofNullable(
                        jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_MAX_ID_SQL, table), Long.class))
                .orElse(0L);
    }

    public long getMaxIdByProject(String project, long from) {
        return Optional
                .ofNullable(jdbcTemplate.queryForObject(
                        String.format(Locale.ROOT, SELECT_MAX_ID_BY_PROJECT_SQL, table, from, project), Long.class))
                .orElse(0L);
    }

    @Override
    public long getMinId() {
        return Optional
                .ofNullable(
                        jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_MIN_ID_SQL, table), Long.class))
                .orElse(0L);
    }

    public long count(long startId, long endId) {
        return jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_COUNT_ID_RANGE, table, startId, endId),
                Long.class);
    }

    @Override
    public void restore(long currentId) {
        if (config.isJobNode() && !config.isUTEnv()) {
            log.info("current maxId is {}", currentId);
            replayWorker.startSchedule(currentId, false);
            return;
        }
        // query node need wait update to latest due to restore from backup
        replayWorker.startSchedule(currentId, true);
    }

    @Override
    public void catchupWithTimeout() throws Exception {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
        replayWorker.waitForCatchup(getMaxId(), config.getCatchUpTimeout());
    }

    public void catchupWithMaxTimeout() throws Exception {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
        replayWorker.waitForCatchup(getMaxId(), config.getCatchUpMaxTimeout());
    }

    @Override
    public void catchup() {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
    }

    @Override
    public void forceCatchup() {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.forceCatchFrom(store.getOffset());
    }

    @Override
    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public AuditLog get(String resPath, long mvcc) {
        return withTransaction(transactionManager, () -> {
            val result = jdbcTemplate.query(
                    String.format(Locale.ROOT, SELECT_BY_META_KET_AND_MVCC, table, resPath, mvcc),
                    new AuditLogRowMapper());
            if (!result.isEmpty()) {
                return result.get(0);
            }
            return null;
        });
    }

    @Override
    public void rotate() {
        withTransaction(transactionManager, () -> {
            val maxSize = config.getMetadataAuditLogMaxSize();
            val deletableMaxId = getMaxId() - maxSize + 1;
            log.info("try to delete audit_logs which id less than {}", deletableMaxId);
            jdbcTemplate.update(String.format(Locale.ROOT, DELETE_ID_LESSTHAN_SQL, table), deletableMaxId);
            return null;
        });
    }

    private Properties loadMedataProperties() throws IOException {
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
        return properties;
    }

    void createTableIfNotExist() throws SQLException, IOException {
        if (isTableExists(jdbcTemplate.getDataSource().getConnection(), table)) {
            return;
        }
        Properties properties = loadMedataProperties();
        var sql = properties.getProperty(CREATE_TABLE);

        jdbcTemplate.execute(String.format(Locale.ROOT, sql, table, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT,
                AUDIT_LOG_TABLE_TS, AUDIT_LOG_TABLE_MVCC));
        log.info("Succeed to create table: {}", table);
    }

    void createIndexIfNotExist() {
        try {
            String indexName = String.format(Locale.ROOT, META_KEY_META_MVCC_INDEX, table);
            if (isIndexExists(jdbcTemplate.getDataSource().getConnection(), table, indexName)) {
                return;
            }
            Properties properties = loadMedataProperties();
            var sql = properties.getProperty(META_KEY_META_MVCC_INDEX_KEY);

            jdbcTemplate.execute(String.format(Locale.ROOT, sql, indexName, table));
            log.info("Succeed to create table {} index: {}", table, indexName);
        } catch (Exception e) {
            log.warn("Failed create index on table {}", table, e);
        }
    }

    void createIfNotExist() throws Exception {
       createTableIfNotExist();
       createIndexIfNotExist();
    }

    @Override
    public void close() throws IOException {
        replayWorker.close(false);
    }

    @VisibleForTesting
    public void forceClose() {
        replayWorker.close(true);
    }

    public long getLogOffset() {
        return replayWorker.getLogOffset();
    }

}
