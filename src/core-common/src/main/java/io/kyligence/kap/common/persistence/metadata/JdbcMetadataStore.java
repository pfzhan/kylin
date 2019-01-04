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

import java.io.File;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Optional;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.metadata.jdbc.RawResourceRowMapper;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcMetadataStore extends MetadataStore {

    private static final RowMapper<RawResource> RAW_RESOURCE_ROW_MAPPER = new RawResourceRowMapper();

    private static final String META_TABLE_KEY = "META_TABLE_KEY";
    private static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";
    private static final String META_TABLE_TS = "META_TABLE_TS";
    private static final String META_TABLE_MVCC = "META_TABLE_MVCC";

    private static final String SELECT_TERM = "select ";

    private static final String COUNT_ALL_SQL = "select count(1) from %s";
    private static final String SELECT_ALL_KEY_SQL = SELECT_TERM + META_TABLE_KEY + " from %s";
    private static final String SELECT_BY_PAGE_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s order by " + META_TABLE_KEY + " limit %s offset %s";
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

    private final DataSourceTransactionManager transactionManager;
    private final JdbcTemplate jdbcTemplate;
    private final String table;

    public JdbcMetadataStore(KylinConfig config, String namespace) {
        super(config, namespace);
        val url = config.getMetadataUrl();
        val dataSource = new BasicDataSource();
        dataSource.setDriverClassName(
                Optional.ofNullable(url.getParameter("driverClassName")).orElse("com.mysql.jdbc.Driver"));
        dataSource.setUrl(Optional.ofNullable(url.getParameter("url")).orElse("jdbc:mysql://sandbox/kylin"));
        dataSource.setUsername(Optional.ofNullable(url.getParameter("username")).orElse("root"));
        dataSource.setPassword(Optional.ofNullable(url.getParameter("password")).orElse(""));
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier() + namespace.replaceAll("/", "_");

        createIfNotExist();
    }

    @Override
    protected void save(String path, ByteSource bs, long ts, long mvcc) throws Exception {
        withTransaction(() -> {
            if (bs != null) {
                val result = jdbcTemplate.query(String.format(SELECT_BY_KEY_MVCC_SQL, table, path, mvcc - 1),
                        RAW_RESOURCE_ROW_MAPPER);
                if (CollectionUtils.isEmpty(result)) {
                    jdbcTemplate.update(String.format(INSERT_SQL, table), path, bs.read(), ts, mvcc);
                } else {
                    jdbcTemplate.update(String.format(UPDATE_SQL, table), bs.read(), mvcc, ts, path, mvcc - 1);
                }
            } else {
                jdbcTemplate.update(String.format(DELETE_SQL, table), path);
            }
            return null;
        });
    }

    @Override
    protected NavigableSet<String> list(String rootPath) {
        val allPaths = withTransaction(
                () -> jdbcTemplate.queryForList(String.format(SELECT_ALL_KEY_SQL, table), String.class));
        return Sets.newTreeSet(allPaths);
    }

    @Override
    protected RawResource load(String path) throws IOException {
        return withTransaction(() -> jdbcTemplate.queryForObject(String.format(SELECT_BY_KEY_SQL, table, path),
                RAW_RESOURCE_ROW_MAPPER));
    }

    @Override
    public void batchUpdate(UnitMessages unitMessages) throws Exception {
        withTransaction(() -> {
            super.batchUpdate(unitMessages);
            return null;
        });
    }

    @Override
    public void restore(ResourceStore store) throws IOException {
        val rowMapper = new RawResourceRowMapper();
        withTransaction(() -> {
            long count = jdbcTemplate.queryForObject(String.format(COUNT_ALL_SQL, table), Long.class);
            long offset = 0;
            long pageSize = 1000;
            while (offset < count) {
                for (RawResource resource : jdbcTemplate
                        .query(String.format(SELECT_BY_PAGE_SQL, table, pageSize, offset), rowMapper)) {
                    store.putResourceWithoutCheck(resource.getResPath(), resource.getByteSource(), resource.getMvcc());
                }
                offset += pageSize;
            }
            return null;
        });
    }

    @Override
    public void dump(ResourceStore store) throws Exception {
        withTransaction(() -> {
            super.dump(store);
            return null;
        });
    }

    @Override
    public void uploadFromFile(File folder) {
        withTransaction(() -> {
            super.uploadFromFile(folder);
            return null;
        });
    }

    private void createIfNotExist() {
        jdbcTemplate.execute(String.format(
                "create table if not exists %s ( %s varchar(255) primary key, %s longblob, %s bigint, %s bigint)",
                table, META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC));
    }

    private <T> T withTransaction(Callback<T> consumer) {
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        val status = transactionManager.getTransaction(definition);
        try {
            T result = consumer.handle();
            transactionManager.commit(status);
            return result;
        } catch (Exception e) {
            transactionManager.rollback(status);
            throw new PersistException("persist messages failed", e);
        }
    }

    private interface Callback<T> {
        T handle() throws Exception;
    }
}
