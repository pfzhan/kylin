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

package io.kyligence.kap.clickhouse.job;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import lombok.val;
import org.mockito.Mockito;

public class BlobUrlTest {

    @After
    public void teardown() {
        Mockito.clearAllCaches();
    }

    @Test
    public void fromHttpUrl() {
        // case 1
        String accountKey = "testkey";
        val config = Maps.<String, String>newHashMap();
        config.put("fs.azure.account.key.account.blob.core.chinacloudapi.cn", accountKey);
        HadoopMockUtil.mockGetConfiguration(config);
        BlobUrl blobUrl = BlobUrl.fromHttpUrl("https://account.blob.core.chinacloudapi.cn/container/blob.parquet");
        Assert.assertEquals("account", blobUrl.getAccountName());
        Assert.assertEquals("blob.core.chinacloudapi.cn", blobUrl.getHostSuffix());
        Assert.assertEquals("container", blobUrl.getContainer());
        Assert.assertEquals("https", blobUrl.getHttpSchema());
        Assert.assertEquals("wasbs", blobUrl.getBlobSchema());
        Assert.assertEquals("/blob.parquet", blobUrl.getPath());
        Assert.assertEquals(accountKey, blobUrl.getAccountKey());

        // case2
        BlobUrl blobUrl2 = BlobUrl.fromHttpUrl("http://account.blob.core.chinacloudapi.cn/container/blob.parquet");
        Assert.assertEquals("wasb", blobUrl2.getBlobSchema());
        Assert.assertEquals("http://account.blob.core.chinacloudapi.cn", blobUrl2.getHttpEndpoint());
    }

    @Test
    public void fromBlobUrl() {
        // case1
        String accountKey = "testkey";
        val config = Maps.<String, String>newHashMap();
        config.put("fs.azure.account.key.account.blob.core.chinacloudapi.cn", accountKey);
        HadoopMockUtil.mockGetConfiguration(config);
        BlobUrl blobUrl = BlobUrl.fromBlobUrl("wasbs://container@account.blob.core.chinacloudapi.cn/blob.parquet");
        Assert.assertEquals("account", blobUrl.getAccountName());
        Assert.assertEquals("blob.core.chinacloudapi.cn", blobUrl.getHostSuffix());
        Assert.assertEquals("container", blobUrl.getContainer());
        Assert.assertEquals("https", blobUrl.getHttpSchema());
        Assert.assertEquals("wasbs", blobUrl.getBlobSchema());
        Assert.assertEquals("/blob.parquet", blobUrl.getPath());
        Assert.assertEquals(accountKey, blobUrl.getAccountKey());

        // case2
        BlobUrl blobUrl2 = BlobUrl.fromBlobUrl("wasb://container@account.blob.core.chinacloudapi.cn/blob.parquet");
        Assert.assertEquals("http", blobUrl2.getHttpSchema());
        Assert.assertEquals("DefaultEndpointsProtocol=http;AccountName=account;AccountKey=testkey;EndpointSuffix=core.chinacloudapi.cn", blobUrl2.getConnectionString());
    }
}