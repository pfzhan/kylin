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

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BinaryLogicalFilterTest {
    private static BinaryFilter[] children;

    @BeforeClass
    public static void setUp() throws Exception {
        children = new BinaryFilter[2];
        children[0] = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x30 }), 1, 1);
        children[1] = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.NEQ, Lists.newArrayList(new byte[] { 0x40 }), 2, 1);
    }

    @Test
    public void isAndMatch() throws Exception {
        BinaryFilter filter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.AND, children);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x44 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x40 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x40 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x40 })));
    }

    @Test
    public void isOrMatch() throws Exception {
        BinaryFilter filter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.OR, children);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x44 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x40 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x44 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x40 })));
    }

    @Test
    public void mixMatch() throws Exception {
        BinaryFilter andFilter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.AND, children);
        BinaryFilter equalFilter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x50 }), 3, 1);
        BinaryFilter orFilter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.OR, andFilter, equalFilter);

        Assert.assertTrue(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x44, 0x50 })));
        Assert.assertTrue(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x44, 0x50 })));
        Assert.assertTrue(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x30, 0x44, 0x00 })));
        Assert.assertFalse(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x33, 0x44, 0x00 })));
    }
}