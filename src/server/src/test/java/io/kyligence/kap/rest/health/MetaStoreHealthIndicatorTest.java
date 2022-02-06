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
package io.kyligence.kap.rest.health;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Health;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;


public class MetaStoreHealthIndicatorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testHealth() {
        MetaStoreHealthIndicator indicator = Mockito.spy(new MetaStoreHealthIndicator());
        ReflectionTestUtils.setField(indicator, "isHealth", true);
        Assert.assertEquals(Health.up().build().getStatus(), indicator.health().getStatus());

        ReflectionTestUtils.setField(indicator, "isHealth", false);
        Assert.assertEquals(Health.down().build().getStatus(), indicator.health().getStatus());
    }

    @Test
    public void testHealthCheck() {
        MetaStoreHealthIndicator indicator = Mockito.spy(new MetaStoreHealthIndicator());
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        indicator.healthCheck();
        Assert.assertTrue((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        Mockito.doThrow(RuntimeException.class).when(indicator).allNodeCheck();
        indicator.healthCheck();
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        Mockito.doReturn(null).when(indicator).allNodeCheck();
        indicator.healthCheck();
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));
    }

}
