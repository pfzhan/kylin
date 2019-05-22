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

import static io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore.datasourceParameters;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Joiner;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.metadata.jdbc.AuditLogRowMapper;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcTransactionMixin;
import io.kyligence.kap.common.persistence.transaction.MessageSynchronization;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogStore implements AuditLogStore, JdbcTransactionMixin {

    static final String AUDIT_LOG_SUFFIX = "_audit_log";

    static final String AUDIT_LOG_TABLE_KEY = "meta_key";
    static final String AUDIT_LOG_TABLE_CONTENT = "meta_content";
    static final String AUDIT_LOG_TABLE_TS = "meta_ts";
    static final String AUDIT_LOG_TABLE_MVCC = "meta_mvcc";
    static final String AUDIT_LOG_TABLE_UNIT = "unit_id";
    static final String AUDIT_LOG_TABLE_OPERATOR = "operator";

    static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR)
            + ") values (?, ?, ?, ?, ?, ?)";
    static final String SELECT_BY_RANGE_SQL = "select "
            + Joiner.on(",").join("id", AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR)
            + " from %s where id > %d and id <= %d order by id";
    static final String SELECT_MAX_ID_SQL = "select max(id) from %s";
    static final String DELETE_ID_LESSTHAN_SQL = "delete from %s where id < ?";

    private final KylinConfig config;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    private final String table;
    @Getter
    private final DataSourceTransactionManager transactionManager;
    private ExecutorService consumeExecutor;

    public JdbcAuditLogStore(KylinConfig config) throws Exception {
        this.config = config;
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier() + AUDIT_LOG_SUFFIX;

        createIfNotExist();
    }

    public JdbcAuditLogStore(KylinConfig config, JdbcTemplate jdbcTemplate,
            DataSourceTransactionManager transactionManager, String table) {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionManager = transactionManager;
        this.table = table;

        createIfNotExist();
    }

    public void save(UnitMessages unitMessages) {
        val unitId = unitMessages.getUnitId();
        val operator = Optional.ofNullable(SecurityContextHolder.getContext().getAuthentication())
                .map(Principal::getName).orElse(null);
        withTransaction(() -> jdbcTemplate.batchUpdate(String.format(INSERT_SQL, table),
                unitMessages.getMessages().stream().map(e -> {
                    if (e instanceof ResourceCreateOrUpdateEvent) {
                        ResourceCreateOrUpdateEvent createEvent = (ResourceCreateOrUpdateEvent) e;
                        try {
                            return new Object[] { createEvent.getResPath(),
                                    createEvent.getCreatedOrUpdated().getByteSource().read(),
                                    createEvent.getCreatedOrUpdated().getTimestamp(),
                                    createEvent.getCreatedOrUpdated().getMvcc(), unitId, operator };
                        } catch (IOException ignore) {
                            return null;
                        }
                    } else if (e instanceof ResourceDeleteEvent) {
                        ResourceDeleteEvent deleteEvent = (ResourceDeleteEvent) e;
                        return new Object[] { deleteEvent.getResPath(), null, null, null, unitId, operator };
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList())));
    }

    public List<AuditLog> fetch(long currentId, long size) {
        log.trace("fetch log from {} < id <= {}", currentId, currentId + size);
        return jdbcTemplate.query(String.format(SELECT_BY_RANGE_SQL, table, currentId, currentId + size),
                new AuditLogRowMapper());
    }

    @Override
    public long getMaxId() {
        return Optional.ofNullable(jdbcTemplate.queryForObject(String.format(SELECT_MAX_ID_SQL, table), Long.class))
                .orElse(0L);
    }

    @Override
    public void restore(ResourceStore store, long currentId) {
        val replayer = MessageSynchronization.getInstance(config);
        consumeExecutor = Executors.newSingleThreadExecutor();
        withTransaction(() -> {
            val step = 1000L;
            val maxId = getMaxId();
            log.debug("start restore, current max_id is {}", maxId);
            var startId = currentId;
            UnitMessages messages = new UnitMessages();
            while (startId < maxId) {
                val logs = fetch(startId, Math.min(step, maxId - startId));
                messages = replayLogs(replayer, logs, messages);
                startId += step;
            }
            replayer.replay(messages);
            consumeExecutor.submit(createFetcher(maxId));
            return null;
        });
    }

    @Override
    public void rotate() {
        withTransaction(() -> {
            val maxSize = config.getMetadataAuditLogMaxSize();
            val deletableMaxId = getMaxId() - maxSize + 1;
            log.info("try to delete audit_logs which id less than {}", deletableMaxId);
            jdbcTemplate.update(String.format(DELETE_ID_LESSTHAN_SQL, table), deletableMaxId);
            return null;
        });
    }

    private Runnable createFetcher(long id) {
        return () -> {
            val replayer = MessageSynchronization.getInstance(config);
            long startId = id;
            boolean stop = false;
            while (!stop) {
                try {
                    long finalStartId = startId;
                    startId = withTransaction(() -> {
                        val logs = fetch(finalStartId, Integer.MAX_VALUE);
                        if (CollectionUtils.isEmpty(logs)) {
                            return finalStartId;
                        }
                        val messages = replayLogs(replayer, logs, new UnitMessages());
                        replayer.replay(messages);
                        return logs.get(logs.size() - 1).getId();
                    });
                    if (startId == finalStartId) {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("thread interrupted", e);
                } catch (Exception e) {
                    log.error("unknown exception, exit ", e);
                    stop = true;
                    if (!config.isUTEnv()) {
                        System.exit(-2);
                    }
                }
            }
        };
    }

    private UnitMessages replayLogs(MessageSynchronization replayer, List<AuditLog> logs, UnitMessages messages) {
        var currentUnitId = "";
        for (AuditLog log : logs) {
            val event = Event.fromLog(log);
            if (Objects.equals(currentUnitId, log.getUnitId())) {
                messages.getMessages().add(event);
            } else {
                replayer.replay(messages);
                messages = new UnitMessages();
                currentUnitId = log.getUnitId();
                messages.getMessages().add(event);
            }
        }
        return messages;
    }

    void createIfNotExist() {
        var sql = "create table if not exists %s ( id bigint auto_increment primary key, %s varchar(255)";
        if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("com.mysql.jdbc.Driver")) {
            sql += " COLLATE utf8_bin";
        }
        sql += ", %s longblob, %s bigint, %s bigint, unit_id varchar(50), operator varchar(100))";
        jdbcTemplate.execute(String.format(sql, table, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                AUDIT_LOG_TABLE_MVCC));
    }

    @Override
    public void close() throws IOException {
        ExecutorServiceUtil.forceShutdown(consumeExecutor);
    }
}
