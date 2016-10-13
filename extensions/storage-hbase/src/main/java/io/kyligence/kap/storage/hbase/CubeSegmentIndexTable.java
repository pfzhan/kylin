/**
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

package io.kyligence.kap.storage.hbase;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.gridtable.GTScanRanges;
import io.kyligence.kap.cube.gridtable.GTUtilExd;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.cube.index.IIndexTable;

public class CubeSegmentIndexTable implements IIndexTable {

    private CubeSegmentIndexReaderManager indexManager;
    private GTInfo info;
    private CubeSegment cubeSegment;
    private Cuboid cuboid;

    public CubeSegmentIndexTable(CubeSegment cubeSegment, GTInfo info, Cuboid cuboid) {
        this.info = info;
        this.cubeSegment = cubeSegment;
        this.cuboid = cuboid;
        this.indexManager = CubeSegmentIndexReaderManager.getInstance(this.cubeSegment);
    }

    private GTScanRanges buildPositiveGTScanRanges(IColumnInvertedIndex.Reader invertedIndex, TblColRef column, Set<Integer> conditionValues) {
        TreeSet<Integer> rowSet = Sets.newTreeSet();
        for (int conditionValue : conditionValues) {
            int[] rows = invertedIndex.getRows(conditionValue).toArray();
            for (int r : rows) {
                rowSet.add(r);
            }
        }

        List<Range<Integer>> rowRanges = RangeUtil.buildRanges(rowSet);
        GTRecord[] startKeys = new GTRecord[rowRanges.size()];
        GTRecord[] endKeys = new GTRecord[rowRanges.size()];
        int[] rowNums = new int[rowRanges.size()];
        for (int i = 0; i < rowRanges.size(); i++) {
            startKeys[i] = new GTRecord(info);
            endKeys[i] = new GTRecord(info);
            rowNums[i] = 0;
        }

        List<TblColRef> dimensionColumns = cuboid.getColumns();
        ImmutableBitSet primaryKey = info.getPrimaryKey();
        for (int i = 0; i < dimensionColumns.size(); i++) {
            int col = primaryKey.trueBitAt(i);

            DimensionEncoding dimEn = info.getCodeSystem().getDimEnc(col);
            int dimLength = dimEn.getLengthOfEncoding();

            IColumnForwardIndex.Reader forwardIndexReader = indexManager.getColumnForwardIndex(dimensionColumns.get(i));
            if (forwardIndexReader != null) {
                for (int r = 0; r < rowRanges.size(); r++) {
                    byte[] buffer = new byte[dimLength];
                    BytesUtil.writeUnsigned(forwardIndexReader.get(rowRanges.get(r).lowerEndpoint()), buffer, 0, dimLength);
                    startKeys[r].set(col, new ByteArray(buffer));

                    buffer = new byte[dimLength];
                    BytesUtil.writeUnsigned(forwardIndexReader.get(rowRanges.get(r).upperEndpoint()), buffer, 0, dimLength);
                    endKeys[r].set(col, new ByteArray(buffer));
                }
            }
        }

        return new GTScanRanges(startKeys, endKeys, rowNums);
    }

    @Override
    public GTScanRanges lookup(CompareTupleFilter filter) {
        GTScanRanges scanRanges = new GTScanRanges();
        try {
            TblColRef column = filter.getColumn();
            // convert to real column if it's mockup column
            if (column.getTable().equals("NULL.GT_MOCKUP_TABLE")) {
                column = GTUtilExd.getRealColFromMockUp(column, cuboid);
            }

            // TODO: Currently only dict dimensions are supported.
            Dictionary dict = cubeSegment.getDictionary(column);
            if (dict == null) {
                throw new RuntimeException("Only dictionary dimensions are supported.");
            }

            IColumnInvertedIndex.Reader invertedIndex = indexManager.getColumnInvertedIndex(column);

            switch (filter.getOperator()) {
            case IN:
            case EQ:
                Set<Integer> intVals = Sets.newHashSet();
                for (ByteArray byteVal : (Set<ByteArray>) filter.getValues()) {
                    intVals.add(BytesUtil.readUnsigned(byteVal.array(), 0, byteVal.length()));
                }
                scanRanges = buildPositiveGTScanRanges(invertedIndex, column, intVals);
                break;
            case ISNULL:
                scanRanges = buildPositiveGTScanRanges(invertedIndex, column, Sets.newHashSet(dict.nullId()));
                break;

            case GT:
            case GTE:
                int gtValue = BytesUtil.readUnsigned(((ByteArray) filter.getFirstValue()).asBuffer(), dict.getSizeOfId());
                Set<Integer> gtValues = Range.closed(gtValue, dict.getMaxId()).asSet(DiscreteDomains.integers());
                scanRanges = buildPositiveGTScanRanges(invertedIndex, column, gtValues);
                break;

            case LT:
            case LTE:
                int ltValue = BytesUtil.readUnsigned(((ByteArray) filter.getFirstValue()).asBuffer(), dict.getSizeOfId());
                Set<Integer> ltValues = Range.closed(dict.getMinId(), ltValue).asSet(DiscreteDomains.integers());
                scanRanges = buildPositiveGTScanRanges(invertedIndex, column, ltValues);
                break;

            // in fact, currently it's [closed,closed], these will return all.
            case NEQ:
            case NOTIN:
            case ISNOTNULL:
                GTRecord nullRecord = new GTRecord(info);
                scanRanges.addScanRage(new GTScanRanges.ScanRange(nullRecord, nullRecord, 0));
                break;

            default:
                throw new RuntimeException("Unsupported Operator: " + filter.getOperator());
            }
            scanRanges.optimize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return scanRanges;
    }
}
