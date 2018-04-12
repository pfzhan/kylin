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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FederatedResourceStoreTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final ResourceStore base = ResourceStore.getKylinMetaStore(config);
        final File deleDir = File.createTempFile("FederatedResourceStoreTest", "tmp");
        final String delePath = "/delegate";
        final String project = "testprj";

        String projectPath = "/" + project;
        String delegateDirectory = projectPath + delePath;
        String resPath = delegateDirectory + "/test.json";

        deleDir.delete(); // we need a dir, not a file

        try {
            ResourceStore deleStore = HDFSResourceStore.newLocalStore(config, deleDir);
            FederatedResourceStore fed = new FederatedResourceStore(base, ImmutableMap.of(delePath, deleStore));

            // no delegated resource at first
            Assert.assertFalse(fed.listResources("/").contains(delegateDirectory));
            Assert.assertNull(fed.listResources(delegateDirectory));

            // add a delegated resource, check it is on disk
            fed.putResource(resPath, new StringEntity("{some text}"), StringEntity.serializer);
            Assert.assertTrue(!base.exists(resPath));
            Assert.assertTrue(deleStore.exists(resPath));
            Assert.assertTrue(new File(deleDir, resPath.substring(1)).exists());

            // check listing reflects delegated resource
            Assert.assertTrue(fed.listResources(projectPath).contains(delegateDirectory));
            Assert.assertTrue(fed.listResources(delegateDirectory).size() == 1);
            Assert.assertTrue(fed.listResources(delegateDirectory).contains(resPath));

            // test read
            StringEntity entity = fed.getResource(resPath, StringEntity.class, StringEntity.serializer);
            Assert.assertTrue(entity.toString().equals("{some text}"));

            // delete and check it is gone on disk
            fed.deleteResource(resPath);
            Assert.assertFalse(deleStore.exists(resPath));
            Assert.assertFalse(new File(deleDir, resPath.substring(1)).exists());

            // delete the path
            new File(deleDir, delegateDirectory).delete();
            Assert.assertFalse(fed.listResources("/").contains(delegateDirectory));
            Assert.assertNull(fed.listResources(delegateDirectory));

            // delegated resource in base is not visible
            base.putResource(resPath, new StringEntity("{some text}"), StringEntity.serializer);
            Assert.assertFalse(fed.listResources("/").contains(delePath));
            Assert.assertNull(fed.listResources(delePath));

        } finally {
            FileUtils.forceDelete(deleDir);
        }

    }

}
