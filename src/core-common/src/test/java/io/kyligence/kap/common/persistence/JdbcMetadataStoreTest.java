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
package io.kyligence.kap.common.persistence;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CompressionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.google.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.metadata.jdbc.RawResourceRowMapper;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class JdbcMetadataStoreTest extends NLocalFileMetadataTestCase {

    private static int index = 0;
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setMetadataUrl("test" + index
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        index++;
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc2", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc2".getBytes(DEFAULT_CHARSET)), 0);
            store.checkAndPutResource("/p1/abc4", ByteSource.wrap("abc2".getBytes(DEFAULT_CHARSET)), 1000L,
                    -1);
            store.deleteResource("/p1/abc");
            return 0;
        }, "p1");
        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);
        val all = jdbcTemplate.query("select * from " + url.getIdentifier(), new RawResourceRowMapper());
        Assert.assertEquals(3, all.size());
        for (RawResource resource : all) {
            if (resource.getResPath().equals("/p1/abc2")) {
                Assert.assertEquals(0, resource.getMvcc());
            }
            if (resource.getResPath().equals("/p1/abc3")) {
                Assert.assertEquals(1, resource.getMvcc());
            }
            if (resource.getResPath().equals("/p1/abc4")) {
                Assert.assertEquals(1000L, resource.getTimestamp());
            }
        }
    }

    @Test
    public void testPage() throws Exception {
        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        val tableName = url.getIdentifier();
        jdbcTemplate.execute(String.format(Locale.ROOT,
                "create table if not exists %s ( META_TABLE_KEY varchar(255) primary key, META_TABLE_CONTENT longblob, META_TABLE_TS bigint,  META_TABLE_MVCC bigint)",
                tableName));

        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                Lists.newArrayList(
                        new Object[] { "/_global/project/p0.json", "project".getBytes(DEFAULT_CHARSET),
                                System.currentTimeMillis(), 0L },
                        new Object[] { "/_global/project/p1.json", "project".getBytes(DEFAULT_CHARSET),
                                System.currentTimeMillis(), 0L }));
        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                IntStream.range(0, 2048)
                        .mapToObj(i -> new Object[] { "/p" + (i / 1024) + "/res" + i,
                                ("content" + i).getBytes(DEFAULT_CHARSET), System.currentTimeMillis(), 0L })
                        .collect(Collectors.toList()));

        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        Assert.assertEquals(1024, resourceStore.listResourcesRecursively("/p0").size());
        Assert.assertEquals(1024, resourceStore.listResourcesRecursively("/p1").size());
    }

    @Test
    @Ignore("for develop")
    public void testDuplicate() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), 0);
            return 0;
        }, "p1");

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.update("update " + url.getIdentifier() + " set META_TABLE_MVCC = 10");

        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), 1);
            return 0;
        }, "p1");
    }

    @Test
    public void testBatchUpdate() throws Exception {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(
                new RawResource("/p1/test", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.batchUpdate(unitMessages, false, "/p1/test", -1);
            return null;
        }, "p1");

        String content = new String(metadataStore.load("/p1/test").getByteSource().read());
        Assert.assertEquals("test content", content);

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        val tableName = url.getIdentifier();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        byte[] contents = jdbcTemplate.queryForObject(
                "select META_TABLE_CONTENT from " + tableName + " where META_TABLE_KEY = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertFalse(CompressionUtils.isCompressed(contents));

        byte[] auditLogContents = jdbcTemplate.queryForObject(
                "select meta_content from " + tableName + "_audit_log where meta_key = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertFalse(CompressionUtils.isCompressed(auditLogContents));
    }

    @Test
    public void testBatchUpdateWithMetadataCompressEnable() throws Exception {
        overwriteSystemProp("kylin.metadata.compress.enabled", "true");
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(
                new RawResource("/p1/test", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.batchUpdate(unitMessages, false, "/p1/test", -1);
            return null;
        }, "p1");

        String content = new String(metadataStore.load("/p1/test").getByteSource().read());
        Assert.assertEquals("test content", content);

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        val tableName = url.getIdentifier();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        byte[] contents = jdbcTemplate.queryForObject(
                "select META_TABLE_CONTENT from " + tableName + " where META_TABLE_KEY = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertTrue(CompressionUtils.isCompressed(contents));
        byte[] gzip = "GZIP".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(gzip, Arrays.copyOf(contents, gzip.length));
        Assert.assertArrayEquals("test content".getBytes(StandardCharsets.UTF_8),
                CompressionUtils.decompress(contents));

        byte[] auditLogContents = jdbcTemplate.queryForObject(
                "select meta_content from " + tableName + "_audit_log where meta_key = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertTrue(CompressionUtils.isCompressed(auditLogContents));
        Assert.assertArrayEquals(gzip, Arrays.copyOf(auditLogContents, gzip.length));
        Assert.assertArrayEquals("test content".getBytes(StandardCharsets.UTF_8),
                CompressionUtils.decompress(auditLogContents));
    }

}
