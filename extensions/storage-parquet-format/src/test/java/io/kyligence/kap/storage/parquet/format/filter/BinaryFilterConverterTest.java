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

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class BinaryFilterConverterTest extends LocalFileMetadataTestCase {
    private CubeSegment seg;
    private CubeDesc desc;
    private Cuboid cuboid;
    private BinaryFilterConverter converter;
    private ByteArray expectedValue;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_empty");
        cube.getModel().setPartitionDesc(new PartitionDesc());
        seg = mgr.appendSegment(cube);
        desc = seg.getCubeDesc();
        cuboid = Cuboid.findById(seg, (long) 0xff);
        converter = new BinaryFilterConverter(seg, cuboid);
        expectedValue = new ByteArray(new byte[] { 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f });
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void toConstantBinaryFilter() throws UnsupportedEncodingException {
        ConstantTupleFilter tupleFilter = ConstantTupleFilter.TRUE;
        BinaryFilter binaryFilter = converter.toBinaryFilter(tupleFilter);
        Assert.assertTrue(binaryFilter.isMatch(null));
    }

    @Test
    public void toEqBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x05, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.EQ);
    }

    @Test
    public void toNeqBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x44, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.NEQ);
    }

    @Test
    public void toLtBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.LT);
    }

    @Test
    public void toGtBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x07, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.GT);
    }

    @Test
    public void toLteBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x06, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.LTE);
        evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, null, TupleFilter.FilterOperatorEnum.LTE);
    }

    @Test
    public void toGteBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.GTE);
        evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x07, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        binaryAssert(evaluateValue, null, TupleFilter.FilterOperatorEnum.GTE);
    }

    @Test
    public void toIsNullBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, (byte) 0x0f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.ISNULL);
    }

    @Test
    public void toIsNotNullBinaryFilter() throws Exception {
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, (byte) 0x0f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x10 };
        byte[] failEvaluateValue = new byte[] { 0x01, 0x02, 0x03, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x10 };
        binaryAssert(evaluateValue, failEvaluateValue, TupleFilter.FilterOperatorEnum.ISNOTNULL);
    }

    @Test
    public void toInBinaryFilter() throws Exception {
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(seg));
        int index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[5].getColRef());

        ByteArray expectedValue2 = new ByteArray(new byte[] { 0x05, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f });
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        byte[] evaluateValue2 = new byte[] { 0x01, 0x02, 0x03, 0x05, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.IN);
        filter.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        filter.addChild(new ConstantTupleFilter(Lists.newArrayList(expectedValue, expectedValue2)));
        BinaryFilter binaryFilter = converter.toBinaryFilter(filter);

        Assert.assertEquals(TupleFilter.FilterOperatorEnum.IN, binaryFilter.getOperator());
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue2)));
    }

    @Test
    public void toNotInBinaryFilter() throws Exception {
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(seg));
        int index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[5].getColRef());

        ByteArray expectedValue2 = new ByteArray(new byte[] { 0x05, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f });
        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x06, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NOTIN);
        filter.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        filter.addChild(new ConstantTupleFilter(Lists.newArrayList(expectedValue, expectedValue2)));
        BinaryFilter binaryFilter = converter.toBinaryFilter(filter);

        Assert.assertEquals(TupleFilter.FilterOperatorEnum.NOTIN, binaryFilter.getOperator());
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
    }

    @Test
    public void toAndBinaryFilter() throws Exception {
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(seg));

        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        TupleFilter eqfilter1 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        int index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[0].getColRef());
        eqfilter1.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        eqfilter1.addChild(new ConstantTupleFilter(new ByteArray(new byte[] { 0x01, 0x02, 0x03 })));

        TupleFilter eqfilter2 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[5].getColRef());
        eqfilter2.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        eqfilter2.addChild(new ConstantTupleFilter(expectedValue));

        TupleFilter logicalFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        logicalFilter.addChild(eqfilter1);
        logicalFilter.addChild(eqfilter2);

        BinaryFilter binaryFilter = converter.toBinaryFilter(logicalFilter);
        Assert.assertEquals(TupleFilter.FilterOperatorEnum.AND, binaryFilter.getOperator());
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        evaluateValue = new byte[] { 0x01, 0x01, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        Assert.assertFalse(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        Assert.assertFalse(binaryFilter.isMatch(new ByteArray(evaluateValue)));
    }

    @Test
    public void toOrBinaryFilter() throws Exception {
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(seg));

        byte[] evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        TupleFilter eqfilter1 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);

        int index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[0].getColRef());
        eqfilter1.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        eqfilter1.addChild(new ConstantTupleFilter(new ByteArray(new byte[] { 0x01, 0x02, 0x03 })));

        index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[5].getColRef());
        TupleFilter eqfilter2 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        eqfilter2.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        eqfilter2.addChild(new ConstantTupleFilter(expectedValue));

        TupleFilter logicalFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        logicalFilter.addChild(eqfilter1);
        logicalFilter.addChild(eqfilter2);

        BinaryFilter binaryFilter = converter.toBinaryFilter(logicalFilter);
        Assert.assertEquals(TupleFilter.FilterOperatorEnum.OR, binaryFilter.getOperator());
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        evaluateValue = new byte[] { 0x01, 0x01, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        evaluateValue = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        evaluateValue = new byte[] { 0x01, 0x01, 0x03, 0x04, 0x04, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        Assert.assertFalse(binaryFilter.isMatch(new ByteArray(evaluateValue)));
    }

    private void binaryAssert(byte[] evaluateValue, byte[] failEvaluateValue, TupleFilter.FilterOperatorEnum filterOp) throws UnsupportedEncodingException {
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(seg));
        int index = mapping.indexOf(desc.getRowkey().getRowKeyColumns()[5].getColRef());

        TupleFilter filter = new CompareTupleFilter(filterOp);
        filter.addChild(new ColumnTupleFilter(gtInfo.colRef(index)));
        filter.addChild(new ConstantTupleFilter(expectedValue));
        BinaryFilter binaryFilter = converter.toBinaryFilter(filter);

        Assert.assertEquals(filterOp, binaryFilter.getOperator());
        Assert.assertTrue(binaryFilter.isMatch(new ByteArray(evaluateValue)));
        if (failEvaluateValue != null) {
            Assert.assertFalse(binaryFilter.isMatch(new ByteArray(failEvaluateValue)));
        }
    }
}
