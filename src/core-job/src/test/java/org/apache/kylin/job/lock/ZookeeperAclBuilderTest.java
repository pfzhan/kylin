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

package org.apache.kylin.job.lock;

import static junit.framework.TestCase.fail;

import java.util.List;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.hadoop.util.ZKUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ZookeeperAclBuilderTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testAclEnabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "true");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        Assert.assertNotNull(zookeeperAclBuilder);
        Assert.assertTrue(zookeeperAclBuilder.isNeedAcl());

        List<ACL> zkAcls = Lists.newArrayList();
        try {
            zkAcls = ZookeeperAclBuilder.getZKAcls();
            Assert.assertFalse(zkAcls.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read ACLs based on 'kylin.env.zookeeper.zk-acl' in kylin.properties");
        }

        List<ZKUtil.ZKAuthInfo> zkAuthInfo = Lists.newArrayList();
        try {
            zkAuthInfo = ZookeeperAclBuilder.getZKAuths();
            Assert.assertFalse(zkAuthInfo.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read Auth based on 'kylin.env.zookeeper.zk-auth' in kylin.properties");
        }

        Builder builder = zookeeperAclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder());
        Assert.assertNotNull(builder);
        Assert.assertEquals(zkAcls, builder.getAclProvider().getDefaultAcl());
        Assert.assertNotNull(builder.getAuthInfos());
    }

    @Test
    public void testAclDisabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "false");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        Assert.assertNotNull(zookeeperAclBuilder);
        Assert.assertFalse(zookeeperAclBuilder.isNeedAcl());

        Builder builder = zookeeperAclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder());
        Assert.assertNotNull(builder);
        Assert.assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, builder.getAclProvider().getDefaultAcl());
        Assert.assertNull(builder.getAuthInfos());
    }

}
