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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
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
            resourceStore.checkAndPutResource("/_global/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/_global/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/_global/path/to/res3", ByteStreams.asByteSource("{}".getBytes()), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res3").getMvcc());
    }

    @Test
    public void testExceptionInTransactionWithRetry() {
        try {
            val ret = UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                resourceStore.checkAndPutResource("/_global/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore.checkAndPutResource("/_global/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
                throw new IllegalArgumentException("surprise");
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception ignore) {
        }

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(null, resourceStore.getResource("/_global/path/to/res"));
        Assert.assertEquals(null, resourceStore.getResource("/_global/path/to/res2"));

        // test can be used again after exception
        testTransaction();
    }

    @Test
    public void testUnitOfWorkPreprocess() {
        class A implements UnitOfWork.Callback<Object> {
            private List<String> list = Lists.newArrayList();

            @Override
            public String toString() {
                return list.size() + "";
            }

            @Override
            public void preProcess() {
                try {
                    throw new Throwable("no args");
                } catch (Throwable e) {
                    list.add(e.getMessage());
                }
            }

            @Override
            public Object process() {
                list.add(this.toString());
                throw new IllegalStateException("conflict");
            }

            @Override
            public void onProcessError(Throwable throwable) {
                list.add("conflict");
            }
        }
        A callback = new A();
        Assert.assertTrue(callback.list.isEmpty());
        try {
            val ret = UnitOfWork.doInTransactionWithRetry(callback, UnitOfWork.GLOBAL_UNIT);
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof TransactionException);
            Assert.assertEquals("conflict", Throwables.getRootCause(e).getMessage());
        }
        Assert.assertEquals(7, callback.list.size());
        Assert.assertEquals("no args", callback.list.get(0));
        Assert.assertEquals("1", callback.list.get(1));
        Assert.assertEquals("no args", callback.list.get(2));
        Assert.assertEquals("3", callback.list.get(3));
        Assert.assertEquals("no args", callback.list.get(4));
        Assert.assertEquals("5", callback.list.get(5));
        Assert.assertEquals("conflict", callback.list.get(6));
    }

    @Test
    public void testReentrant() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/_global/path/to/res", ByteStreams.asByteSource("{}".getBytes()), -1L);
            resourceStore.checkAndPutResource("/_global/path/to/res2", ByteStreams.asByteSource("{}".getBytes()), -1L);
            UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore2 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                resourceStore2.checkAndPutResource("/_global/path2/to/res2/1", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore2.checkAndPutResource("/_global/path2/to/res2/2", ByteStreams.asByteSource("{}".getBytes()), -1L);
                resourceStore2.checkAndPutResource("/_global/path2/to/res2/3", ByteStreams.asByteSource("{}".getBytes()), -1L);
                Assert.assertEquals(resourceStore, resourceStore2);
                return 0;
            }, UnitOfWork.GLOBAL_UNIT);
            resourceStore.checkAndPutResource("/_global/path/to/res3", ByteStreams.asByteSource("{}".getBytes()), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path2/to/res2/1").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path2/to/res2/2").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path2/to/res2/3").getMvcc());
        Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res3").getMvcc());
    }

    @Test
    public void testReadLockExclusive() {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        resourceStore.checkAndPutResource("/_global/path/to/res1", ByteStreams.asByteSource("{}".getBytes()), -1L);
        Object condition = new Object();
        AtomicBoolean stop = new AtomicBoolean();
        Thread readLockHelder = new Thread(
            () -> {
                UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(
                        () -> {
                            synchronized (condition) {
                              condition.notify();
                            }
                            boolean interrupted = false;
                            while (!interrupted && !Thread.interrupted() && !stop.get()) {
                                synchronized (condition) {
                                    condition.notify();
                                }
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    interrupted = true;
                                }
                            }
                            return 0;
                        }).build());
            }
        );
        readLockHelder.start();
        synchronized (condition) {
            try {
                condition.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long readStart = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(() -> {
                        long cost = System.currentTimeMillis() - readStart;
                        Assert.assertTrue(cost < 500);
                        Assert.assertEquals(0,
                                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource("/_global/path/to/res1").getMvcc());
                        return 0;
                    }).build());
        } catch (Exception e) {
            Assert.fail();
        }
        new Thread(
                () -> {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    stop.set(true);
                }
        ).start();
        long writeStart = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(false).maxRetry(1).processor(() -> {
                        long cost = System.currentTimeMillis() - writeStart;
                        Assert.assertTrue(cost > 1500);
                        Assert.assertEquals(0,
                                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource("/_global/path/to/res1").getMvcc());
                        return 0;
                    }).build());
        } catch (Exception e) {
            Assert.fail();
        }
        stop.set(true);
    }

    @Test
    public void testWriteLockExclusive() {
        Object condition = new Object();
        AtomicBoolean stop = new AtomicBoolean();
        Thread writeLockHelder = new Thread(
            () -> {
                UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(false).maxRetry(1).processor(
                        () -> {
                            val resourceStoreInTransaction = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                            resourceStoreInTransaction.checkAndPutResource("/_global/path/to/res1", ByteStreams.asByteSource("{}".getBytes()), -1L);
                            synchronized (condition) {
                                condition.notify();
                            }
                            boolean interrupted = false;
                            while (!interrupted && !Thread.interrupted() && !stop.get()) {
                                synchronized (condition) {
                                    condition.notify();
                                }
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    interrupted = true;
                                }
                            }
                            synchronized (condition) {
                                condition.notify();
                            }
                            return 0;
                        }).build());
            }
        );
        writeLockHelder.start();
        synchronized (condition) {
            try {
                condition.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        new Thread(
            () -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stop.set(true);
            }
        ).start();
        long start = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                .readonly(true).maxRetry(1).processor(() -> {
                    long cost = System.currentTimeMillis() - start;
                    Assert.assertTrue(cost > 1500);
                    Assert.assertEquals(0,
                            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource("/_global/path/to/res1").getMvcc());
                    return 0;
                }).build());
        } catch (Exception e) {
            Assert.fail();
        }
        stop.set(true);
    }

    @Test
    public void testUpdateInReadTransaction() {
        try {
            System.setProperty("kylin.env", "PROD");
            UnitOfWork.doInTransactionWithRetry(
                UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                        .readonly(true).maxRetry(1).processor(() -> {
                    val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    resourceStore.checkAndPutResource("/_global/path/to/res1", ByteStreams.asByteSource("{}".getBytes()), -1L);
                    return 0;
                }).build());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(TransactionException.class, e.getClass());
        } finally {
            System.clearProperty("kylin.env");
        }
    }

    @Test
    public void testReadTransaction() {
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .checkAndPutResource("/_global/path/to/res1", ByteStreams.asByteSource("{}".getBytes()), -1L);
        UnitOfWork.doInTransactionWithRetry(
            UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(() -> {
                val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                Assert.assertEquals(0, resourceStore.getResource("/_global/path/to/res1").getMvcc());
                return 0;
            }).build());
    }

    @Test
    public void testWriteTransaction() {

        UnitOfWork.doInTransactionWithRetry(
                UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                        .readonly(false).maxRetry(1).processor(() -> {
                    val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    resourceStore.checkAndPutResource("/_global/path/to/res1", ByteStreams.asByteSource("{}".getBytes()), -1L);
                    return 0;
                }).build());
        Assert.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getResource("/_global/path/to/res1").getMvcc());

    }
}
