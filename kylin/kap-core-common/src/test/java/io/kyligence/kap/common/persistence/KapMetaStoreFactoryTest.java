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

import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.FederatedResourceStore;
import org.apache.kylin.common.persistence.HDFSResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class KapMetaStoreFactoryTest extends NLocalFileMetadataTestCase {
    
    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testDictionaryStoreSeparation() {
        ResourceStore store = ResourceStore.getKylinMetaStore(getTestConfig());
        FederatedResourceStore fed = (FederatedResourceStore) store;
        
        HDFSResourceStore dictStore = (HDFSResourceStore) fed.getDelegates().get(ResourceStore.DICT_RESOURCE_ROOT);
        HDFSResourceStore snapshotStore = (HDFSResourceStore) fed.getDelegates().get(ResourceStore.SNAPSHOT_RESOURCE_ROOT);
        
        Assert.assertTrue(dictStore == snapshotStore);
        StorageURL storageUrl = dictStore.getStorageUrl();
        String dictPath = storageUrl.getParameter("path");
        Assert.assertTrue(dictPath.startsWith("file:/"));
        Assert.assertTrue(dictPath.endsWith("/dict-store"));
    }
}
