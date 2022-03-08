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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static org.mockito.Mockito.when;


public class TableSourceTypeTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void s3() {
        String result = TableSourceType.S3.transformFileUrl("hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals("S3('http://host.docker.internal:9000/liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet' , 'test','test123', Parquet)", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void hdfs() {
        TableSourceType.HDFS.transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unSupportedFormat() {
        TableSourceType.HDFS.transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test
    public void hdfsNoException() {
        String url = TableSourceType.HDFS.transformFileUrl("hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals("HDFS('hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet' , Parquet)", url);
    }

    @Test
    public void viewfs() {
        FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
        try{
            when(mockFs.resolvePath(new Path("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet"))).thenReturn(new Path("hdfs://hdfstest/snap.parquet"));
        }catch (IOException e){
            Assert.fail();
        }

        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Class c = viewfs.getClass();

        try{
            Field field = c.getDeclaredField("vfs");
            field.setAccessible(true);
            field.set(viewfs, mockFs);
        }catch (IllegalAccessException| NoSuchFieldException | SecurityException e){
            Assert.fail();
        }

        Assert.assertEquals(TableEngineType.HDFS, TableSourceType.HDFS.getTableEngineType());
        String result = TableSourceType.HDFS.transformFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals("HDFS('hdfs://hdfstest/snap.parquet' , Parquet)", result);

    }

    @Test
    public void TableSourceTest() {
        val sourceType = Lists.newArrayList(TableSourceType.HDFS, TableSourceType.S3, TableSourceType.BLOB, TableSourceType.UT);
        for (val type : sourceType){
            if (type == TableSourceType.S3) {
                Assert.assertEquals(TableEngineType.S3, type.getTableEngineType());
                val expected = type.transformFileUrl("hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
                val actual = TableSourceType.S3.transformFileUrl("hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
                Assert.assertEquals(expected, actual);
            } else if (type == TableSourceType.HDFS) {
                Assert.assertEquals(TableEngineType.HDFS, type.getTableEngineType());
            }else if (type == TableSourceType.UT) {
                Assert.assertEquals(TableEngineType.URL, type.getTableEngineType());
                val expected = type.transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
                val actual = TableSourceType.UT.transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
                Assert.assertEquals(expected, actual);
            }

        }
    }
}