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

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.exception.KylinException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.mockito.Mockito.when;

public class ViewFsTransformTest {

    @Test
    public void testGetInstance() {
        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Assert.assertNotNull(viewfs);
    }

    @Test
    public void testGenerateFileUrl() {
        FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
        try{
            when(mockFs.resolvePath(new Path("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet"))).thenReturn(new Path("hdfs://hdfstest/snap.parquet"));
        }catch (IOException e){
            Assert.fail();
        }

        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Assert.assertNotNull(viewfs);
        Class c = viewfs.getClass();

        try{
            Field field = c.getDeclaredField("vfs");
            field.setAccessible(true);
            field.set(viewfs, mockFs);
        }catch (IllegalAccessException| NoSuchFieldException | SecurityException e){
            Assert.fail();
        }

        String url = ViewFsTransform.getInstance().generateFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet");
        Assert.assertEquals("hdfs://hdfstest/snap.parquet", url);
    }

    @Test(expected = KylinException.class)
    public void testGenerateFileFailUrl() {
        ViewFsTransform.getInstance().generateFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet");
    }
}