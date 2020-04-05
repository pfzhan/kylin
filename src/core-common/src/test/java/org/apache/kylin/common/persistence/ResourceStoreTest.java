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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.common.persistence;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class ResourceStoreTest extends NLocalFileMetadataTestCase {
    @Before
    public void setupResource() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateMetaStoreUuidIfNotExist() {
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .deleteResource(ResourceStore.METASTORE_UUID_TAG);
        Set<String> res = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).listResources("/");
        Assert.assertFalse("failed", res.contains(ResourceStore.METASTORE_UUID_TAG));
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
        res = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).listResources("/");
        Assert.assertTrue("failed", res.contains(ResourceStore.METASTORE_UUID_TAG));
    }

    @Test
    public void testCreateMetaStoreUuidUsingTwoKylinConfig() {
        KylinConfig A = KylinConfig.getInstanceFromEnv();
        ResourceStore AStore = ResourceStore.getKylinMetaStore(A);//system store
        KylinConfig B = KylinConfig.createKylinConfig(A);
        ResourceStore BStore = ResourceStore.getKylinMetaStore(B);//new store

        //delete AStore,BStore uuid
        ResourceStore.getKylinMetaStore(A).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(A).deleteResource(ResourceStore.METASTORE_UUID_TAG);
        ResourceStore.getKylinMetaStore(B).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(B).deleteResource(ResourceStore.METASTORE_UUID_TAG);
        Set<String> res = AStore.listResources("/");
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));
        res = BStore.listResources("/");
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));

        //create BStore uuid
        BStore.createMetaStoreUuidIfNotExist();
        res = BStore.listResources("/");
        Assert.assertTrue(res.contains(ResourceStore.METASTORE_UUID_TAG));//B have uuid 
        res = AStore.listResources("/");
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));//A did not

        //try create again
        AStore.createMetaStoreUuidIfNotExist();
        BStore.createMetaStoreUuidIfNotExist();

    }
}
