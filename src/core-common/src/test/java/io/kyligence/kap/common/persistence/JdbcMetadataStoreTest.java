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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.metadata.jdbc.RawResourceRowMapper;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class JdbcMetadataStoreTest extends NLocalFileMetadataTestCase {

    private static int index = 0;

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
            store.checkAndPutResource("/p1/abc", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc2", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc3", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc3", ByteStreams.asByteSource("abc2".getBytes()), 0);
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
        Assert.assertEquals(2, all.size());
        for (RawResource resource : all) {
            if (resource.getResPath().equals("/p1/abc2")) {
                Assert.assertEquals(0, resource.getMvcc());
            }
            if (resource.getResPath().equals("/p1/abc3")) {
                Assert.assertEquals(1, resource.getMvcc());
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
        jdbcTemplate.execute(String.format(
                "create table if not exists %s ( META_TABLE_KEY varchar(255) primary key, META_TABLE_CONTENT longblob, META_TABLE_TS bigint,  META_TABLE_MVCC bigint)",
                tableName));

        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                Lists.newArrayList(
                        new Object[] { "/_global/project/p0.json", "project".getBytes(), System.currentTimeMillis(),
                                0L },
                        new Object[] { "/_global/project/p1.json", "project".getBytes(), System.currentTimeMillis(),
                                0L }));
        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                IntStream
                        .range(0, 2048).mapToObj(i -> new Object[] { "/p" + (i / 1024) + "/res" + i,
                                ("content" + i).getBytes(), System.currentTimeMillis(), 0L })
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
            store.checkAndPutResource("/p1/abc", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc", ByteStreams.asByteSource("abc".getBytes()), 0);
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
            store.checkAndPutResource("/p1/abc", ByteStreams.asByteSource("abc".getBytes()), 1);
            return 0;
        }, "p1");
    }

}
