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
package io.kyligence.kap.common.persistence.metadata.epochstore;

import java.util.Arrays;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public abstract class AbstractEpochStoreTest extends NLocalFileMetadataTestCase {

    EpochStore epochStore;

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannnot init epoch store!");
        }
    }

    boolean compareEpoch(Epoch a, Epoch b) {
        return Objects.equals(a.getCurrentEpochOwner(), b.getCurrentEpochOwner())
                && Objects.equals(a.getEpochTarget(), b.getEpochTarget())
                && Objects.equals(a.getEpochId(), b.getEpochId())
                && Objects.equals(a.getLastEpochRenewTime(), b.getLastEpochRenewTime());
    }

    @Test
    public void testInsertAndUpdate() {

        Epoch newSaveEpoch = new Epoch();
        newSaveEpoch.setEpochTarget("test1");
        newSaveEpoch.setCurrentEpochOwner("owner1");
        newSaveEpoch.setEpochId(1);
        newSaveEpoch.setLastEpochRenewTime(System.currentTimeMillis());

        //insert one
        epochStore.insert(newSaveEpoch);
        val epochs = epochStore.list();
        Assert.assertEquals(epochs.size(), 1);

        Assert.assertTrue(compareEpoch(newSaveEpoch, epochs.get(0)));

        //update owner
        newSaveEpoch.setCurrentEpochOwner("o2");
        epochStore.update(newSaveEpoch);

        Assert.assertEquals(newSaveEpoch.getCurrentEpochOwner(), epochStore.list().get(0).getCurrentEpochOwner());

    }

    @Test
    public void testExecuteWithTransaction_Success() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        epochStore.executeWithTransaction(() -> {
            epochStore.insert(e1);

            //insert success
            Assert.assertEquals(epochStore.list().size(), 1);
            Assert.assertTrue(compareEpoch(e1, epochStore.list().get(0)));

            return null;
        });

    }

    @Test
    public void testBatchUpdate() {
        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        epochStore.insert(e1);

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());
        epochStore.insert(e2);

        val batchEpochs = Arrays.asList(e1, e2);

        epochStore.updateBatch(batchEpochs);

        batchEpochs.forEach(epoch -> {
            Assert.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget())));
        });

    }

    @Test
    public void testBatchInsert() {
        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());

        val batchEpochs = Arrays.asList(e1, e2);

        epochStore.insertBatch(batchEpochs);

        batchEpochs.forEach(epoch -> {
            Assert.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget())));
        });

    }
}
