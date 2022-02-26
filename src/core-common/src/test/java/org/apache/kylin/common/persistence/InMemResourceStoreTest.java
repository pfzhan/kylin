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

package org.apache.kylin.common.persistence;

import java.io.IOException;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTestBase.TestEntity;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import lombok.val;

@MetadataInfo(onlyProps = true)
@OverwriteProp(key = "kylin.env", value = "UT")
class InMemResourceStoreTest {

    @Test
    public void testFileStore() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.wrapInNewUrl(config.getMetadataUrl().toString(), config,
                ResourceStoreTestBase::testAStore);
    }

    @Test
    public void testMemoryLeak() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.testPotentialMemoryLeak(config.getMetadataUrl().toString(), config);
    }

    @Test
    public void testUUID() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.wrapInNewUrl(config.getMetadataUrl().toString(), config,
                ResourceStoreTestBase::testGetUUID);
    }

    @Test
    void testReload() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore systemResourceStore = ResourceStore.getKylinMetaStore(config);

        final JsonSerializer<TestEntity> serializer = new JsonSerializer<>(TestEntity.class);
        systemResourceStore.checkAndPutResource("/test", new TestEntity("data2"), serializer);

        KylinConfig newConfig = KylinConfig.createKylinConfig(config);
        ResourceStore copyResourceStore = ResourceStore.getKylinMetaStore(newConfig);
        Assert.assertFalse(isSameResourceStore(systemResourceStore, copyResourceStore));

        systemResourceStore.reload();
        Assert.assertTrue(isSameResourceStore(systemResourceStore, copyResourceStore));
    }

    private boolean isSameResourceStore(ResourceStore resourceStore1, ResourceStore resourceStore2) {
        val paths1 = resourceStore1.listResourcesRecursively("/");
        val paths2 = resourceStore2.listResourcesRecursively("/");
        return Objects.equals(paths1, paths2);
    }

}
