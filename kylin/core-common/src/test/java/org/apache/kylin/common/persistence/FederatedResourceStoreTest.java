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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FederatedResourceStoreTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = getTestConfig();
        final ResourceStore base = ResourceStore.getKylinMetaStore(config);
        final File deleDir = File.createTempFile("FederatedResourceStoreTest", "tmp");
        final String delePath = "/delegate";
        deleDir.delete(); // we need a dir, not a file
        
        try {
            ResourceStore deleStore = HDFSResourceStore.newLocalStore(config, deleDir);
            FederatedResourceStore fed = new FederatedResourceStore(base, ImmutableMap.of(delePath, deleStore));
            
            // at the beginning, no deleted resource
            Assert.assertTrue(fed.listResources("/").contains(delePath) == false);
            Assert.assertTrue(fed.listResources(delePath) == null);
            
            // add a delegated resource, check it is on disk
            fed.putResource(delePath + "/test", new StringEntity("some text"), StringEntity.serializer);
            Assert.assertTrue(base.exists(delePath + "/test") == false);
            Assert.assertTrue(deleStore.exists(delePath + "/test"));
            Assert.assertTrue(new File(deleDir, delePath.substring(1) + "/test").exists());
            
            // check listing reflects delegated resource
            Assert.assertTrue(fed.listResources("/").contains(delePath));
            Assert.assertTrue(fed.listResources(delePath).size() == 1);
            Assert.assertTrue(fed.listResources(delePath).contains(delePath + "/test"));
            
            // test read
            StringEntity entity = fed.getResource(delePath + "/test", StringEntity.class, StringEntity.serializer);
            Assert.assertTrue(entity.toString().equals("some text"));
            
            // delete and check it is gone on disk
            fed.deleteResource(delePath + "/test");
            Assert.assertTrue(deleStore.exists(delePath + "/test") == false);
            Assert.assertTrue(new File(deleDir, delePath.substring(1) + "/test").exists() == false);
            new File(deleDir, delePath.substring(1)).delete();
            Assert.assertTrue(fed.listResources("/").contains(delePath) == false);
            Assert.assertTrue(fed.listResources(delePath) == null);

            // delegated resource in base is not visible
            base.putResource(delePath + "/test", new StringEntity("some text"), StringEntity.serializer);
            Assert.assertTrue(fed.listResources("/").contains(delePath) == false);
            Assert.assertTrue(fed.listResources(delePath) == null);
            
            // non-delegated resource in deleStore is not visible
            deleStore.putResource("non-dele/test", new StringEntity("some text"), StringEntity.serializer);
            Assert.assertTrue(fed.listResources("/").contains("non-dele") == false);
            Assert.assertTrue(fed.listResources("non-dele") == null);
            
        } finally {
           FileUtils.forceDelete(deleDir);
        }
        
    }
    
}
