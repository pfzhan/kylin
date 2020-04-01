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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.persistence.metadata.jdbc.RawResourceRowMapper;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcMetadataStore extends MetadataStore {

    private static final RowMapper<RawResource> RAW_RESOURCE_ROW_MAPPER = new RawResourceRowMapper();

    static final String META_TABLE_KEY = "META_TABLE_KEY";
    static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";
    static final String META_TABLE_TS = "META_TABLE_TS";
    static final String META_TABLE_MVCC = "META_TABLE_MVCC";

    private static final String SELECT_TERM = "select ";

    private static final String SELECT_ALL_KEY_SQL = SELECT_TERM + META_TABLE_KEY + " from %s";
    private static final String SELECT_BY_RANGE_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + " > '%s' and " + META_TABLE_KEY + " < '%s'";
    private static final String SELECT_BY_KEY_MVCC_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s' and " + META_TABLE_MVCC + "=%d";
    private static final String SELECT_BY_KEY_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s'";

    private static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + ") values (?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update %s set " + META_TABLE_CONTENT + "=?, " + META_TABLE_MVCC + "=?, "
            + META_TABLE_TS + "=? where " + META_TABLE_KEY + "=? and " + META_TABLE_MVCC + "=?";
    private static final String DELETE_SQL = "delete from %s where " + META_TABLE_KEY + "=?";

    @Getter
    private final DataSourceTransactionManager transactionManager;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    private final String table;

    public JdbcMetadataStore(KylinConfig config) throws Exception {
        super(config);
        val url = config.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier();
        if (config.isMetadataAuditLogEnabled()) {
            auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager,
                    table + JdbcAuditLogStore.AUDIT_LOG_SUFFIX);
        }
        createIfNotExist();
    }

    @Override
    protected void save(String path, @Nullable ByteSource bs, long ts, long mvcc) throws Exception {
        withTransaction(transactionManager, new JdbcUtil.Callback<Object>() {
            @Override
            public Object handle() throws Exception {
                if (bs != null) {
                    val result = jdbcTemplate.query(String.format(SELECT_BY_KEY_MVCC_SQL, table, path, mvcc - 1),
                            RAW_RESOURCE_ROW_MAPPER);
                    if (CollectionUtils.isEmpty(result)) {
                        insert(String.format(INSERT_SQL, table), path, bs, ts, mvcc);
                    } else {
                        update(String.format(UPDATE_SQL, table), bs, mvcc, ts, path, mvcc - 1);
                    }
                } else {
                    jdbcTemplate.update(String.format(DELETE_SQL, table), path);
                }
                return null;
            }

            @Override
            public void onError() {
                try {
                    log.warn("write {} {} {} failed", path, mvcc, bs == null ? null : new String(bs.read()));
                } catch (IOException ignore) {
                }
            }
        });
    }

    private void insert(String sql, String path, ByteSource bs, long ts, long mvcc) {
        jdbcTemplate.update(sql, ps -> {
            ps.setString(1, path);
            try {
                ps.setBytes(2, bs.read());
            } catch (IOException e) {
                log.error("exception: ", e);
                throw new SQLException(e);
            }
            ps.setLong(3, ts);
            ps.setLong(4, mvcc);
        });
    }

    private void update(String sql, ByteSource bs, long ts, long mvcc, String path, long oldMvcc) {
        jdbcTemplate.update(sql, ps -> {
            try {
                ps.setBytes(1, bs.read());
            } catch (IOException e) {
                log.error("exception: ", e);
                throw new SQLException(e);
            }
            ps.setLong(2, ts);
            ps.setLong(3, mvcc);
            ps.setString(4, path);
            ps.setLong(5, oldMvcc);
        });
    }

    @Override
    public NavigableSet<String> list(String rootPath) {
        val allPaths = withTransaction(transactionManager,
                () -> jdbcTemplate.queryForList(String.format(SELECT_ALL_KEY_SQL, table), String.class));
        return Sets.newTreeSet(allPaths);
    }

    @Override
    public RawResource load(String path) throws IOException {
        return withTransaction(transactionManager, () -> jdbcTemplate
                .queryForObject(String.format(SELECT_BY_KEY_SQL, table, path), RAW_RESOURCE_ROW_MAPPER));
    }

    @Override
    public void batchUpdate(UnitMessages unitMessages, boolean skipAuditLog) throws Exception {
        if (CollectionUtils.isEmpty(unitMessages.getMessages())) {
            return;
        }
        withTransaction(transactionManager, () -> {
            super.batchUpdate(unitMessages, skipAuditLog);
            return null;
        });
    }

    @Override
    public void restore(ResourceStore store) throws IOException {
        restoreProject(store, "_global");
        val projects = store.listResources("/_global/project");
        Optional.ofNullable(projects).orElse(Sets.newTreeSet()).parallelStream().forEach(projectRes -> {
            val words = projectRes.split("/");
            val project = words[words.length - 1].replace(".json", "");
            restoreProject(store, project);
        });
        try {
            val uuidRaw = load(ResourceStore.METASTORE_UUID_TAG);
            store.putResourceWithoutCheck(uuidRaw.getResPath(), uuidRaw.getByteSource(), uuidRaw.getTimestamp(),
                    uuidRaw.getMvcc());
        } catch (PersistException e) {
            if (e.getCause() instanceof EmptyResultDataAccessException) {
                log.info("Cannot find /UUID in metastore");
            } else {
                throw e;
            }
        }
    }

    @Override
    public void dump(ResourceStore store) throws Exception {
        withTransaction(transactionManager, () -> {
            super.dump(store);
            return null;
        });
    }

    @Override
    public void uploadFromFile(File folder) {
        withTransaction(transactionManager, () -> {
            super.uploadFromFile(folder);
            return null;
        });
    }

    private void restoreProject(ResourceStore store, String project) {
        val rowMapper = new RawResourceRowMapper();
        withTransaction(transactionManager, () -> {
            var prevKey = "/" + project + "/";
            val endKey = "/" + project + "/~";
            val resources = jdbcTemplate.query(String.format(SELECT_BY_RANGE_SQL, table, prevKey, endKey), rowMapper);
            for (RawResource resource : resources) {
                store.putResourceWithoutCheck(resource.getResPath(), resource.getByteSource(), resource.getTimestamp(),
                        resource.getMvcc());
            }
            return null;
        });
    }

    private void createIfNotExist() throws Exception {
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
        var sql = properties.getProperty("create.metadata.store.table");
        jdbcTemplate
                .execute(String.format(sql, table, META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC));
    }

}
