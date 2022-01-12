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

import org.junit.Assert;
import org.junit.Test;

public class SizeConvertUtilTest {

    @Test
    public void testConvert() {
        long size = 15152114135L;
        String convertedSize = SizeConvertUtil.getReadableFileSize(size);
        Assert.assertEquals("14.1 GB", convertedSize);
    }

    @Test
    public void testGB() {
        Assert.assertEquals(100, SizeConvertUtil.byteStringAs("100GB", ByteUnit.GiB));
        Assert.assertEquals(100 * 1024, SizeConvertUtil.byteStringAs("100GB", ByteUnit.MiB));
        Assert.assertEquals(100 * 1024 * 1024, SizeConvertUtil.byteStringAs("100GB", ByteUnit.KiB));
        Assert.assertEquals(100 * 1024 * 1024 * 1024L, SizeConvertUtil.byteStringAs("100GB", ByteUnit.BYTE));
        Assert.assertEquals(100 * 1024 * 1024, SizeConvertUtil.byteStringAs("100m", ByteUnit.BYTE));
        Assert.assertEquals(1, SizeConvertUtil.byteStringAsMb("1024KB"));
    }

    @Test
    public void testByteCountToDisplaySize() {
        Assert.assertEquals("1 B", SizeConvertUtil.byteCountToDisplaySize(1));
        Assert.assertEquals("1.00 KB", SizeConvertUtil.byteCountToDisplaySize(1024));
        Assert.assertEquals("1.00 MB", SizeConvertUtil.byteCountToDisplaySize(1024L * 1024));
        Assert.assertEquals("1.00 GB", SizeConvertUtil.byteCountToDisplaySize(1024L * 1024 * 1024));
        Assert.assertEquals("1.00 TB", SizeConvertUtil.byteCountToDisplaySize(1024L * 1024 * 1024 * 1024));
        Assert.assertEquals("1024.00 TB", SizeConvertUtil.byteCountToDisplaySize(1024L * 1024 * 1024 * 1024 * 1024));

        Assert.assertEquals("2 B", SizeConvertUtil.byteCountToDisplaySize(2));
        Assert.assertEquals("1.00 KB", SizeConvertUtil.byteCountToDisplaySize(1025));
        Assert.assertEquals("1.00 KB", SizeConvertUtil.byteCountToDisplaySize(1025, 2));
        Assert.assertEquals("1.01 KB", SizeConvertUtil.byteCountToDisplaySize(1035));
        Assert.assertEquals("1.01 KB", SizeConvertUtil.byteCountToDisplaySize(1035, 2));
    }
}
