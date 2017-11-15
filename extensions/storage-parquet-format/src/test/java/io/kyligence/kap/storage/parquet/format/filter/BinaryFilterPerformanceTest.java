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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class BinaryFilterPerformanceTest extends LocalFileMetadataTestCase {
    @Ignore
    @Test
    public void TestTraditionalFilter() throws IOException {
        this.createTestMetadata();
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_empty");
        cube.getModel().setPartitionDesc(new PartitionDesc());
        CubeSegment seg = mgr.appendSegment(cube);
        CubeDesc desc = seg.getCubeDesc();
        GTInfo gtInfo = CubeGridTable.newGTInfo(Cuboid.findById(seg, 0xffL), new CubeDimEncMap(seg));
        final GTRecord gtRecord = new GTRecord(gtInfo);
        final Random random = new Random();

        IEvaluatableTuple evaluatableTuple = new IEvaluatableTuple() {
            @Override
            public Object getValue(TblColRef col) {
                // random generate data
                byte[] value = null;
                if (random.nextInt(100) == 1) {
                    value = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };
                } else {
                    value = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
                }
                gtRecord.loadColumns(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7), ByteBuffer.wrap(value));
                return gtRecord.get(0);
            }
        };

        long t = System.currentTimeMillis();
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(desc.getRowkey().getRowKeyColumns()[0].getColRef()));
        filter.addChild(new ConstantTupleFilter(new ByteArray(new byte[] { 0x01, 0x02, 0x03 })));

        IFilterCodeSystem<ByteArray> filterCodeSystem = GTUtil.wrap(gtInfo.getCodeSystem().getComparator());

        //        Assert.assertTrue(filter.evaluate(evaluatableTuple, filterCodeSystem));

        for (int i = 0; i < 1000 * 10000; i++) {
            filter.evaluate(evaluatableTuple, filterCodeSystem);
        }

        System.out.println("takes " + (System.currentTimeMillis() - t) + " ms");
    }

    @Ignore
    @Test
    public void TestBinaryFilter() throws IOException {
        this.createTestMetadata();
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_empty");
        cube.getModel().setPartitionDesc(new PartitionDesc());
        CubeSegment seg = mgr.appendSegment(cube);

        GTInfo gtInfo = CubeGridTable.newGTInfo(Cuboid.findById(seg, 0xffL), new CubeDimEncMap(seg));
        final GTRecord gtRecord = new GTRecord(gtInfo);
        final Random random = new Random();

        long t = System.currentTimeMillis();
        BinaryFilter filter = new BinaryCompareFilter(TupleFilter.FilterOperatorEnum.EQ, Lists.newArrayList(new byte[] { 0x01, 0x02, 0x03 }), 0, 3);
        for (int i = 0; i < 1000 * 10000; i++) {
            if (random.nextInt(100) == 1) {
                byte[] value = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };
                filter.isMatch(new ByteArray(value));
                gtRecord.loadColumns(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7), ByteBuffer.wrap(value));
            } else {
                filter.isMatch(new ByteArray(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f }));
            }
        }

        System.out.println("takes " + (System.currentTimeMillis() - t) + " ms");
    }
}
