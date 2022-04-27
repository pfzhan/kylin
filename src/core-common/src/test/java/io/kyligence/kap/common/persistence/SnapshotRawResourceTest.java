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
package io.kyligence.kap.common.persistence;

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.SnapshotRawResource;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import lombok.val;

@MetadataInfo(project = "ssb")
class SnapshotRawResourceTest {

    @BeforeEach
    void before() throws JsonProcessingException {
        val mockContent = new MockMetaContent("abc", 18);
        val mockContentJson = JsonUtil.writeValueAsBytes(mockContent);
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        resourceStore.putResourceWithoutCheck("/path/meta/abc", ByteSource.wrap(mockContentJson), 123, 101);
    }

    @Test
    void testRawResourceByteSourceSerializer() throws IOException {
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val rawSnapshotRes = new SnapshotRawResource(resourceStore.getResource("/path/meta/abc"));
        val mockContentSer = JsonUtil.readValue(rawSnapshotRes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals("abc", mockContentSer.getName());
        Assertions.assertEquals(18, mockContentSer.getAge());
    }

    @Test
    void testSnapShotRawResourceSerializer() throws IOException {
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val rawSnapshotRes = new SnapshotRawResource(resourceStore.getResource("/path/meta/abc"));

        val snapshotRawJson = JsonUtil.writeValueAsString(rawSnapshotRes);
        Assertions.assertEquals("{\"byte_source\":\"eyJuYW1lIjoiYWJjIiwiYWdlIjoxOH0=\",\"timestamp\":123,\"mvcc\":101}",
                snapshotRawJson);
    }

    @Test
    void testSnapShotRawResourceDeSerializer() throws IOException {
        val snapshotRawResDes = JsonUtil
                .readValue("{\"byte_source\":\"eyJuYW1lIjoiYWJjIiwiYWdlIjoxOH0=\",\"timestamp\":123,\"mvcc\":101}"
                        .getBytes(Charset.defaultCharset()), SnapshotRawResource.class);

        Assertions.assertEquals(101, snapshotRawResDes.getMvcc());
        Assertions.assertEquals(123, snapshotRawResDes.getTimestamp());

        val mockContentDeSer = JsonUtil.readValue(snapshotRawResDes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals(18, mockContentDeSer.getAge());
        Assertions.assertEquals("abc", mockContentDeSer.getName());
    }
}
