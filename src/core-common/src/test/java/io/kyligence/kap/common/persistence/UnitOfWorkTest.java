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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class UnitOfWorkTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testTransaction() {
        val ret = UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/path/to/res3", ByteStreams.asByteSource("{}".getBytes()), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res3").getMvcc());
    }

    @Test
    public void testExceptionInTransactionWithRetry() {
        try {
            val ret = UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                resourceStore.checkAndPutResource("/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore.checkAndPutResource("/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
                throw new IllegalArgumentException("surprise");
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception ignore) {
        }

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(null, resourceStore.getResource("/path/to/res"));
        Assert.assertEquals(null, resourceStore.getResource("/path/to/res2"));

        // test can be used again after exception
        testTransaction();
    }

    @Test
    public void testReentrant() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
            UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore2 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                resourceStore2.checkAndPutResource("/path/to/res2/1", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore2.checkAndPutResource("/path/to/res2/2", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore2.checkAndPutResource("/path/to/res2/3", ByteStreams.asByteSource("{}".getBytes()), -1L);
                Assert.assertEquals(resourceStore, resourceStore2);
                return 0;
            }, UnitOfWork.GLOBAL_UNIT);
            resourceStore.checkAndPutResource("/path/to/res3", ByteStreams.asByteSource("{}".getBytes()), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res2/1").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res2/2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res2/3").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/path/to/res3").getMvcc());
    }

}
