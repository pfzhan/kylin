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

package io.kyligence.kap.storage.parquet.format.filter;

import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BinaryCompareFilterTest {
    private byte[] value = new byte[] { 0x20, 0x30 };
    private List<byte[]> inValues = Lists.newArrayList(new byte[] { 0x20, 0x30 }, new byte[] { 0x40, 0x50 });

    @Test
    public void isEqMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
    }

    @Test
    public void isNeqMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.NEQ, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
    }

    @Test
    public void isLtMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.LT, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x29 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x19, 0x29 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x09, 0x22, 0x30 })));
    }

    @Test
    public void isLteMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.LTE, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x29 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x19, 0x29 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x09, 0x22, 0x30 })));
    }

    @Test
    public void isGtMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.GT, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x09, 0x22, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x29 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x19, 0x29 })));
    }

    @Test
    public void isGteMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.GTE, Lists.newArrayList(value), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x33 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x09, 0x22, 0x30 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x20, 0x29 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x10, 0x19, 0x29 })));
    }

    @Test
    public void isNullMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.ISNULL, null, 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, (byte) 0xff, (byte) 0xff })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, (byte) 0x22, (byte) 0xff })));
    }

    @Test
    public void isNotNullMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, null, 1, 2);
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, (byte) 0xff, (byte) 0xff })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, (byte) 0x22, (byte) 0xff })));
    }

    @Test
    public void isInMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.IN, inValues, 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x20, 0x30 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x40, 0x50 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x20, 0x50 })));
    }

    @Test
    public void isNotInMatch() throws Exception {
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.NOTIN, inValues, 1, 2);
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x20, 0x30 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x40, 0x50 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x20, 0x50 })));
    }
}