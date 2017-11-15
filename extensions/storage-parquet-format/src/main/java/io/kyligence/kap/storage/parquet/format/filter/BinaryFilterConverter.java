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
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BinaryFilterConverter {
    public static final Logger logger = LoggerFactory.getLogger(BinaryFilterConverter.class);
    private Map<TblColRef, Integer> offsetMap;
    private Map<TblColRef, Integer> lengthMap;

    //only test
    BinaryFilterConverter(CubeSegment cubeSeg, Cuboid cuboid) {
        RowKeyEncoder encoder = new RowKeyEncoder(cubeSeg, cuboid);
        GTInfo gtInfo = CubeGridTable.newGTInfo(cuboid, new CubeDimEncMap(cubeSeg));
        List<TblColRef> mapping = cuboid.getCuboidToGridTableMapping().getCuboidDimensionsInGTOrder();

        offsetMap = Maps.newHashMap();
        lengthMap = Maps.newHashMap();
        int offset = 0;

        RowKeyColDesc[] rowKeyColDesc = cubeSeg.getCubeDesc().getRowkey().getRowKeyColumns();
        for (RowKeyColDesc column : rowKeyColDesc) {
            int index = mapping.indexOf(column.getColRef());
            if (index < 0) {
                continue;
            }
            int length = encoder.getColumnLength(column.getColRef());
            TblColRef gtColRef = gtInfo.colRef(index);
            offsetMap.put(gtColRef, offset);
            lengthMap.put(gtColRef, length);
            offset += length;
        }
    }

    public BinaryFilterConverter(GTInfo gtInfo) {
        offsetMap = Maps.newHashMap();
        lengthMap = Maps.newHashMap();
        int offset = 0;

        ImmutableBitSet rowkeyBitset = gtInfo.getPrimaryKey();
        for (int i = 0; i < rowkeyBitset.trueBitCount(); i++) {
            int index = rowkeyBitset.trueBitAt(i);
            TblColRef colRef = gtInfo.colRef(index);
            int length = gtInfo.getCodeSystem().maxCodeLength(index);
            offsetMap.put(colRef, offset);
            lengthMap.put(colRef, length);
            offset += length;
        }
    }

    public BinaryFilter toBinaryFilter(TupleFilter tupleFilter) throws UnsupportedEncodingException {
        if (tupleFilter == null || isSpecialFilter(tupleFilter)) {
            throw new IllegalArgumentException("input filter invalid: " + tupleFilter);
        }

        if (tupleFilter instanceof CompareTupleFilter) {
            CompareTupleFilter compareFilter = (CompareTupleFilter) tupleFilter;
            switch (compareFilter.getOperator()) {
            case EQ:
                return transferEq(tupleFilter);
            case NEQ:
                return transferNeq(tupleFilter);
            case LT:
                return transferLt(tupleFilter);
            case GT:
                return transferGt(tupleFilter);
            case LTE:
                return transferLte(tupleFilter);
            case GTE:
                return transferGte(tupleFilter);
            case ISNULL:
                return transferIsnull(tupleFilter);
            case ISNOTNULL:
                return transferIsnotnull(tupleFilter);
            case IN:
                return transferIn(tupleFilter);
            case NOTIN:
                return transferNotin(tupleFilter);
            default:
                throw new IllegalStateException("operator not supported: " + compareFilter.getOperator() + " in " + tupleFilter);
            }
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            List<BinaryFilter> binaryChildren = Lists.newArrayList();
            for (TupleFilter filter : tupleFilter.getChildren()) {
                BinaryFilter child = toBinaryFilter(filter);
                if (child != null) {
                    binaryChildren.add(child);
                } else {
                    throw new IllegalStateException("filter " + filter + " cannot be converted to binary filter");
                }
            }

            switch (tupleFilter.getOperator()) {
            case AND:
                return new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.AND, binaryChildren.toArray(new BinaryFilter[0]));
            case OR:
                return new BinaryLogicalFilter(TupleFilter.FilterOperatorEnum.OR, binaryChildren.toArray(new BinaryFilter[0]));
            default:
                throw new IllegalArgumentException("Operator " + tupleFilter.getOperator() + " is not supported in Binary Filter");
            }
        } else if (tupleFilter instanceof ConstantTupleFilter) {
            if (tupleFilter.evaluate(null, null)) {
                return new BinaryConstantFilter(true);
            } else {
                return new BinaryConstantFilter(false);
            }
        }

        throw new IllegalArgumentException("Tuple Filter " + tupleFilter + " is not supported");
    }

    private static boolean isSpecialFilter(TupleFilter tupleFilter) {
        if (tupleFilter.getOperator() == TupleFilter.FilterOperatorEnum.MASSIN) {
            return true;
        }
        return false;
    }

    public static boolean containsSpecialFilter(TupleFilter filter) {
        if (filter == null || isSpecialFilter(filter)) {
            return true;
        }

        for (TupleFilter child : filter.getChildren()) {
            if (containsSpecialFilter(child)) {
                return true;
            }
        }

        return false;
    }

    private BinaryFilter transferEq(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.EQ);
    }

    private BinaryFilter transferNeq(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.NEQ);
    }

    private BinaryFilter transferLt(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.LT);
    }

    private BinaryFilter transferGt(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.GT);
    }

    private BinaryFilter transferLte(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.LTE);
    }

    private BinaryFilter transferGte(TupleFilter filter) throws UnsupportedEncodingException {
        return transferOneValue(filter, TupleFilter.FilterOperatorEnum.GTE);
    }

    private BinaryFilter transferIsnull(TupleFilter filter) throws UnsupportedEncodingException {
        return transferZeroValue(filter, TupleFilter.FilterOperatorEnum.ISNULL);
    }

    private BinaryFilter transferIsnotnull(TupleFilter filter) throws UnsupportedEncodingException {
        return transferZeroValue(filter, TupleFilter.FilterOperatorEnum.ISNOTNULL);
    }

    private BinaryFilter transferIn(TupleFilter filter) throws UnsupportedEncodingException {
        return transferMultiValue(filter, TupleFilter.FilterOperatorEnum.IN);
    }

    private BinaryFilter transferNotin(TupleFilter filter) throws UnsupportedEncodingException {
        return transferMultiValue(filter, TupleFilter.FilterOperatorEnum.NOTIN);
    }

    private BinaryFilter transferZeroValue(TupleFilter filter, TupleFilter.FilterOperatorEnum op) throws UnsupportedEncodingException {
        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            TblColRef column = compareTupleFilter.getColumn();
            return new BinaryCompareFilter(op, null, offsetMap.get(column), lengthMap.get(column));
        }
        logger.warn("{} is not CompareTupleFilter type", filter);
        return null;
    }

    private BinaryFilter transferOneValue(TupleFilter filter, TupleFilter.FilterOperatorEnum op) throws UnsupportedEncodingException {
        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            TblColRef column = compareTupleFilter.getColumn();
            byte[] valueBytes = decodeOneValue(compareTupleFilter);
            return valueBytes == null ? null : new BinaryCompareFilter(op, Lists.newArrayList(valueBytes), offsetMap.get(column), lengthMap.get(column));
        }
        logger.warn("{} is not CompareTupleFilter type", filter);
        return null;
    }

    private BinaryFilter transferMultiValue(TupleFilter filter, TupleFilter.FilterOperatorEnum op) throws UnsupportedEncodingException {
        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            TblColRef column = compareTupleFilter.getColumn();
            List<byte[]> valueBytesList = decodeMultiValue(compareTupleFilter);
            return new BinaryCompareFilter(op, valueBytesList, offsetMap.get(column), lengthMap.get(column));
        }
        logger.warn("{} is not CompareTupleFilter type", filter);
        return null;
    }

    private byte[] decodeOneValue(CompareTupleFilter filter) throws UnsupportedEncodingException {
        return ((ByteArray) filter.getFirstValue()).array();
    }

    private List<byte[]> decodeMultiValue(CompareTupleFilter filter) throws UnsupportedEncodingException {
        Set<Object> values = (Set<Object>) filter.getValues();
        List<byte[]> valuesBytes = Lists.newArrayList();
        for (Object v : values) {
            valuesBytes.add(((ByteArray) v).array());
        }
        return valuesBytes;
    }
}
