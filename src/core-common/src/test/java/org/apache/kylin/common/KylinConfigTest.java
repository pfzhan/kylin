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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Maps;

public class KylinConfigTest extends HotLoadKylinPropertiesTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        Assert.assertEquals(3, config.getSlowQueryDefaultDetectIntervalSeconds());

        updateProperty("kylin.query.slowquery-detect-interval", "4");
        KylinConfig.getInstanceFromEnv().reloadFromSiteProperties();

        Assert.assertEquals(4, config.getSlowQueryDefaultDetectIntervalSeconds());
    }

    @Test
    public void testGetMetadataUrlPrefix() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        config.setMetadataUrl("testMetaPrefix@hdfs");
        Assert.assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("/kylin/temp");
        Assert.assertEquals("/kylin/temp", config.getMetadataUrlPrefix());
    }

    @Test
    public void testThreadLocalOverride() throws InterruptedException {
        final String metadata1 = "meta1";
        final String metadata2 = "meta2";

        // set system KylinConfig
        KylinConfig sysConfig = KylinConfig.getInstanceFromEnv();
        sysConfig.setMetadataUrl(metadata1);

        Assert.assertEquals(metadata1, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

        // test thread-local override
        KylinConfig threadConfig = KylinConfig.createKylinConfig(new Properties());
        threadConfig.setMetadataUrl(metadata2);

        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(threadConfig)) {

            Assert.assertEquals(metadata2, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

            // other threads still use system KylinConfig
            final String[] holder = new String[1];
            Thread child = new Thread(new Runnable() {
                @Override
                public void run() {
                    holder[0] = KylinConfig.getInstanceFromEnv().getMetadataUrl().toString();
                }
            });
            child.start();
            child.join();
            Assert.assertEquals(metadata1, holder[0]);
        }
    }

    @Test
    public void testOverrideSparkJobJarPath() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String oldSparkJobJarPath = System.getProperty("kylin.engine.spark.job-jar");
        String overrideSparkJobJarPath = oldSparkJobJarPath + "_override";
        conf.overrideSparkJobJarPath(overrideSparkJobJarPath);
        String newSparkJobJarPath = System.getProperty("kylin.engine.spark.job-jar");
        Assert.assertEquals(newSparkJobJarPath, overrideSparkJobJarPath);

        if (StringUtils.isBlank(oldSparkJobJarPath)) {
            oldSparkJobJarPath = "";
        }
        conf.overrideSparkJobJarPath(oldSparkJobJarPath);
    }

    @Test
    public void testGetKylinJobJarPath() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String kylinJobJarPath = conf.getKylinJobJarPath();
        Assert.assertEquals(kylinJobJarPath, "");
    }

    @Test
    public void testPlaceholderReplace() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        {
            kylinConfig.setProperty("ph_1", "${prop_a}/${prop1}");
            Assert.assertEquals("${prop_a}/${prop1}", kylinConfig.getOptional("ph_1"));

            kylinConfig.setProperty("prop_a", "prop_A");
            kylinConfig.setProperty("prop1", "prop_1");
            Assert.assertEquals("prop_A/prop_1", kylinConfig.getOptional("ph_1"));
        }

        {
            kylinConfig.setProperty("ph_2", "${prop2}/${prop_b}");
            Assert.assertEquals("${prop2}/${prop_b}", kylinConfig.getOptional("ph_2"));

            kylinConfig.setProperty("prop_b", "prop_B");
            kylinConfig.setProperty("prop2", "${prop_b}");
            Assert.assertEquals("prop_B/prop_B", kylinConfig.getOptional("ph_2"));
        }

        {
            kylinConfig.setProperty("ph_3", "${prop3}/${prop_c}/xxx/${prop2}/${prop_xxx}");
            Assert.assertEquals("${prop3}/${prop_c}/xxx/prop_B/${prop_xxx}", kylinConfig.getOptional("ph_3"));

            kylinConfig.setProperty("prop_c", "${prop_C}");
            kylinConfig.setProperty("prop3", "prop_3");
            Assert.assertEquals("prop_3/${prop_C}/xxx/prop_B/${prop_xxx}", kylinConfig.getOptional("ph_3"));
        }

        {
            Map<String, String> override = Maps.newHashMap();
            override.put("ph_4", "${prop4}/${prop_d}:${prop3}/${prop_c}");
            KylinConfigExt kylinConfigExt = KylinConfigExt.createInstance(kylinConfig, override);

            Assert.assertEquals("${prop4}/${prop_d}:prop_3/${prop_C}", kylinConfigExt.getOptional("ph_4"));

            kylinConfigExt.getExtendedOverrides().put("prop_d", "prop_D");
            kylinConfigExt.getExtendedOverrides().put("prop4", "${prop_d}");
            Assert.assertEquals("prop_D/prop_D:prop_3/${prop_C}", kylinConfigExt.getOptional("ph_4"));
        }

        {
            kylinConfig.setProperty("ph_5", "${prop5}");
            Assert.assertEquals("${prop5}", kylinConfig.getOptional("ph_5"));

            kylinConfig.setProperty("prop_d", "${ph_5}");
            kylinConfig.setProperty("prop5", "${prop_d}");
            expectedException.expect(IllegalStateException.class);
            expectedException.expectMessage("${prop5}: prop5->prop_d->ph_5");
            kylinConfig.getOptional("ph_5");
        }
    }
}
