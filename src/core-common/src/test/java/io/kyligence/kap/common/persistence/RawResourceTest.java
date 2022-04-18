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

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@MetadataInfo(project = "ssb")
class RawResourceTest {

    @Test
    void testRawResourceByteSourceSerializer() throws IOException {
        val mockContent = new MockMetaContent("abc", 18);
        val mockContentJson = JsonUtil.writeValueAsBytes(mockContent);
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        resourceStore.putResourceWithoutCheck("/path/meta/abc", ByteSource.wrap(mockContentJson), 123, 101);
        val rawRes = resourceStore.getResource("/path/meta/abc");
        val mockContentSer = JsonUtil.readValue(rawRes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals("abc", mockContentSer.getName());
        Assertions.assertEquals(18, mockContentSer.getAge());
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class MockMetaContent {
    @JsonProperty("name")
    private String name;
    @JsonProperty("age")
    private int age;
}
