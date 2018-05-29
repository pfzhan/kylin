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

package io.kyligence.kap.storage.parquet.format.raw;

import org.apache.kylin.common.util.ByteArray;
import org.junit.Assert;
import org.junit.Test;

public class RawTableUtilsTest {
    @Test
    public void shrinkTest() {
        byte[] origin = new byte[] { 0x10 };
        byte[] expected = new byte[] { 0x40 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 8));

        origin = new byte[] { 0x10, 0x13 };
        ByteArray byteArray = new ByteArray(new byte[] { 0x4c, 0x10, 0x13 }, 1, 2);
        expected = new byte[] { 0x41, 0x30 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 16));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 16).toBytes());

        expected = new byte[] { 0x41, 0x20 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 11));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 11).toBytes());

        expected = new byte[] { 0x71 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 8));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 8).toBytes());

        expected = new byte[] { 0x70 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 7));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 7).toBytes());
    }
}
