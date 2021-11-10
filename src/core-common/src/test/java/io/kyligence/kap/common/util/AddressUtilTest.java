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
package io.kyligence.kap.common.util;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class AddressUtilTest extends NLocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private DefaultHostInfoFetcher hostInfoFetcher = new DefaultHostInfoFetcher();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetLocalInstance() {
        val localAddress = AddressUtil.getLocalInstance();
        Assert.assertTrue(localAddress.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    public void testGetZkLocalInstance() {
        val localHost = AddressUtil.getZkLocalInstance();
        Assert.assertTrue(localHost.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    public void testConvertHost() {
        val host = AddressUtil.convertHost("localhost:7070");
        Assert.assertEquals("127.0.0.1:7070", host);
        Assert.assertEquals("127.0.0.1:7070", AddressUtil.convertHost("unknown:7070"));
    }

    @Test
    public void testGetMockPortAddress() {
        val mockAddr = AddressUtil.getMockPortAddress();
        Assert.assertTrue(mockAddr.endsWith(AddressUtil.MAINTAIN_MODE_MOCK_PORT));

    }

    @Test
    public void testGetLocalServerInfo() {
        val servInfo = AddressUtil.getLocalServerInfo();
        Assert.assertTrue(servInfo.startsWith(hostInfoFetcher.getHostname().replaceAll("[^(_a-zA-Z0-9)]", "")));
    }

    @Test
    public void testGetLocalHostExactAddress() {
        val old = getTestConfig().getServerIpAddress();
        val mockIp = "192.168.1.101";
        getTestConfig().setProperty("kylin.env.ip-address", mockIp);
        val servIp = AddressUtil.getLocalHostExactAddress();
        Assert.assertEquals(servIp, mockIp);
        if (!StringUtils.isEmpty(old)) {
            getTestConfig().setProperty("kylin.env.ip-address", old);
        }
    }

    @Test
    public void testIsSameHost() {
        Assert.assertTrue(AddressUtil.isSameHost(hostInfoFetcher.getHostname()));
        Assert.assertFalse(AddressUtil.isSameHost("unknown"));
    }
}
