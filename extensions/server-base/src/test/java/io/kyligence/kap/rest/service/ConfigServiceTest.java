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

package io.kyligence.kap.rest.service;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class ConfigServiceTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void testGetKylinDefaultScopeConfigs() {
        System.clearProperty("kap.version");
        ConfigService configService = new ConfigService();
        Map<String, String> result = configService.getDefaultConfigMap("cube");
        System.out.println(result);
        Assert.assertEquals(12, result.size());
        Assert.assertTrue(result.containsKey("kylin.storage.hbase.compression-codec"));
        Assert.assertEquals("snappy", result.get("kylin.storage.hbase.compression-codec"));

        result = configService.getDefaultConfigMap("project");
        System.out.println(result);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testGetKAPPlusDefaultScopeConfigs() {
        System.setProperty("kap.version", "KAP Plus");
        ConfigService configService = new ConfigService();
        Map<String, String> result = configService.getDefaultConfigMap("cube");
        System.out.println(result);
        Assert.assertEquals(8, result.size());
        Assert.assertFalse(result.containsKey("kylin.storage.hbase.max-region-count"));

        result = configService.getDefaultConfigMap("project");
        System.out.println(result);
        Assert.assertEquals(1, result.size());
        System.clearProperty("kap.version");
    }
}
