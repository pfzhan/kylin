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

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BinaryFilterSerializerTest {
    @Test
    public void constFilterSerialize() {
        BinaryConstantFilter filter = new BinaryConstantFilter(true);
        Assert.assertTrue(filter.isMatch(null));

        byte[] serialized = BinaryFilterSerializer.serialize(filter);
        BinaryFilter deserializeFilter = BinaryFilterSerializer.deserialize(ByteBuffer.wrap(serialized));
        Assert.assertTrue(deserializeFilter.isMatch(null));
    }

    @Test
    public void eqFilterSerialize() throws Exception {
        BinaryCompareFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x02, 0x03 }), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x02 })));

        byte[] serialized = BinaryFilterSerializer.serialize(filter);

        BinaryFilter deserializedFilter = BinaryFilterSerializer.deserialize(ByteBuffer.wrap(serialized));
        Assert.assertTrue(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03 })));
        Assert.assertFalse(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x02 })));
    }

    @Test
    public void inFilterSerialize() throws Exception {
        BinaryCompareFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.IN, Lists.newArrayList(new byte[] { 0x02, 0x03 }, new byte[] { 0x04, 0x05 }), 1, 2);
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03 })));
        Assert.assertTrue(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x04, 0x05 })));
        Assert.assertFalse(filter.isMatch(new ByteArray(new byte[] { 0x00, 0x06, 0x07 })));

        byte[] serialized = BinaryFilterSerializer.serialize(filter);

        BinaryFilter deserializedFilter = BinaryFilterSerializer.deserialize(ByteBuffer.wrap(serialized));
        Assert.assertTrue(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03 })));
        Assert.assertTrue(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x04, 0x05 })));
        Assert.assertFalse(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x06, 0x07 })));
    }

    @Test
    public void mixLogicalCompareFilterSerialize() throws Exception {
        BinaryCompareFilter equalFilter1 = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x02, 0x03 }), 1, 2);
        BinaryCompareFilter equalFilter2 = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x04, 0x05 }), 3, 2);
        BinaryLogicalFilter andFilter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.AND, equalFilter1, equalFilter2);
        BinaryCompareFilter equalFilter3 = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x06, 0x07 }), 5, 2);
        BinaryLogicalFilter orFilter = new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.OR, andFilter, equalFilter3);

        Assert.assertTrue(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03, 0x04, 0x05, 0x00, 0x00 })));
        Assert.assertTrue(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x07 })));
        Assert.assertFalse(orFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })));

        byte[] serialized = BinaryFilterSerializer.serialize(orFilter);

        BinaryFilter deserializedFilter = BinaryFilterSerializer.deserialize(ByteBuffer.wrap(serialized));

        Assert.assertTrue(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x02, 0x03, 0x04, 0x05, 0x00, 0x00 })));
        Assert.assertTrue(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x07 })));
        Assert.assertFalse(deserializedFilter.isMatch(new ByteArray(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })));
    }
}