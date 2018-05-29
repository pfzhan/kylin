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

package org.apache.kylin.common;

import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class KylinConfigTest extends HotLoadKylinPropertiesTestCase {
    @Test
    public void testDuplicateConfig() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String v = config.getJobControllerLock();
        Assert.assertEquals("org.apache.kylin.job.lock.MockJobLock", v);
    }

    @Test
    public void testMRConfigOverride() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = config.getMRConfigOverride();
        Assert.assertEquals(3, override.size());
        Assert.assertEquals("test1", override.get("test1"));
        Assert.assertEquals("test2", override.get("test2"));
        Assert.assertEquals("false", override.get("yarn.timeline-service.enabled"));
    }

    @Test
    public void testBackwardCompatibility() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String oldk = "kylin.test.bcc.old.key";
        final String newk = "kylin.test.bcc.new.key";

        Assert.assertNull(config.getOptional(oldk));
        Assert.assertNotNull(config.getOptional(newk));

        Map<String, String> override = Maps.newHashMap();
        override.put(oldk, "1");
        KylinConfigExt ext = KylinConfigExt.createInstance(config, override);
        Assert.assertEquals(ext.getOptional(oldk), null);
        Assert.assertEquals(ext.getOptional(newk), "1");
        Assert.assertNotEquals(config.getOptional(newk), "1");

        config.setProperty(oldk, "2");
        Assert.assertEquals(config.getOptional(newk), "2");
    }

    @Test
    public void testExtShareTheBase() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = Maps.newHashMap();
        KylinConfig configExt = KylinConfigExt.createInstance(config, override);
        Assert.assertTrue(config.properties == configExt.properties);
        config.setProperty("1234", "1234");
        Assert.assertEquals("1234", configExt.getOptional("1234"));
    }

    @Test
    public void testPropertiesHotLoad() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Assert.assertEquals("whoami@kylin.apache.org", config.getKylinOwner());

        updateProperty("kylin.storage.hbase.owner-tag", "kylin@kylin.apache.org");
        KylinConfig.getInstanceFromEnv().reloadFromSiteProperties();

        Assert.assertEquals("kylin@kylin.apache.org", config.getKylinOwner());
    }

    @Test
    public void testGetMetadataUrlPrefix() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        config.setMetadataUrl("testMetaPrefix@hbase");
        Assert.assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("testMetaPrefix@hdfs");
        Assert.assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("/kylin/temp");
        Assert.assertEquals("/kylin/temp", config.getMetadataUrlPrefix());
    }

    @Test
    public void testThreadLocalOverride() {
        final String metadata1 = "meta1";
        final String metadata2 = "meta2";

        // set system KylinConfig
        KylinConfig sysConfig = KylinConfig.getInstanceFromEnv();
        sysConfig.setMetadataUrl(metadata1);

        Assert.assertEquals(metadata1, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

        // test thread-local override
        KylinConfig threadConfig = KylinConfig.createKylinConfig(new Properties());
        threadConfig.setMetadataUrl(metadata2);
        KylinConfig.setKylinConfigThreadLocal(threadConfig);

        Assert.assertEquals(metadata2, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

        // other threads still use system KylinConfig
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Started new thread.");
                Assert.assertEquals(metadata1, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());
            }
        }).start();
    }

    @Test
    public void testHdfsWorkingDir() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String hdfsWorkingDirectory = conf.getHdfsWorkingDirectory();
        Assert.assertTrue(hdfsWorkingDirectory.startsWith("file:/"));
    }
}
