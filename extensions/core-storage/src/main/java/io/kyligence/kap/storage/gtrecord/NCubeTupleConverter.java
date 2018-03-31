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

package io.kyligence.kap.storage.gtrecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.gtrecord.ITupleConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;

/**
 * Convert Object[] (decoded GTRecord) to tuple
 */
public class NCubeTupleConverter implements ITupleConverter {

    private static final Logger logger = LoggerFactory.getLogger(NCubeTupleConverter.class);

    final NDataSegment dataSegment;
    final NCuboidLayout cuboidLayout;
    final TupleInfo tupleInfo;
    private final List<IDerivedColumnFiller> derivedColFillers;

    private final int[] gtColIdx;
    private final int[] tupleIdx;
    private final MeasureType<?>[] measureTypes;

    private final List<IAdvMeasureFiller> advMeasureFillers;
    private final List<Integer> advMeasureIndexInGTValues;

    private final int nSelectedDims;

    public NCubeTupleConverter(NDataSegment dataSegment, NLayoutCandidate layoutCandidate, //
            Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, int[] gtColIdx,
            TupleInfo returnTupleInfo) {
        this.dataSegment = dataSegment;
        this.cuboidLayout = layoutCandidate.getCuboidLayout();
        this.gtColIdx = gtColIdx;
        this.tupleInfo = returnTupleInfo;
        this.derivedColFillers = Lists.newArrayList();

        nSelectedDims = selectedDimensions.size();
        tupleIdx = new int[selectedDimensions.size() + selectedMetrics.size()];

        // measure types don't have this many, but aligned length make programming easier
        measureTypes = new MeasureType[selectedDimensions.size() + selectedMetrics.size()];

        advMeasureFillers = Lists.newArrayListWithCapacity(1);
        advMeasureIndexInGTValues = Lists.newArrayListWithCapacity(1);

        ////////////

        int i = 0;

        // pre-calculate dimension index mapping to tuple
        for (TblColRef dim : selectedDimensions) {
            tupleIdx[i] = tupleInfo.hasColumn(dim) ? tupleInfo.getColumnIndex(dim) : -1;
            i++;
        }

        for (FunctionDesc metric : selectedMetrics) {
            if (metric.needRewrite()) {
                String rewriteFieldName = metric.getRewriteFieldName();
                tupleIdx[i] = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
            } else {
                // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
                TblColRef col = metric.getParameter().getColRefs().get(0);
                tupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            }

            MeasureType<?> measureType = metric.getMeasureType();
            if (measureType.needAdvancedTupleFilling()) {
                Map<TblColRef, Dictionary<String>> dictionaryMap = buildDictionaryMap(
                        measureType.getColumnsNeedDictionary(metric));
                advMeasureFillers.add(measureType.getAdvancedTupleFiller(metric, returnTupleInfo, dictionaryMap));
                advMeasureIndexInGTValues.add(i);
            } else {
                measureTypes[i] = measureType;
            }

            i++;

        }

        //prepare derived columns and filler

        Map<TblColRef, DeriveInfo> derivedToHostMap = layoutCandidate.getDerivedToHostMap();
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = makeHostToDerivedMap(derivedToHostMap);

        for (Map.Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedInfo.entrySet()) {
            TblColRef[] hostCols = entry.getKey().data;
            for (DeriveInfo deriveInfo : entry.getValue()) {
                IDerivedColumnFiller filler = newDerivedColumnFiller(hostCols, deriveInfo);
                if (filler != null) {
                    derivedColFillers.add(filler);
                }
            }
        }
    }

    private Map<Array<TblColRef>, List<DeriveInfo>> makeHostToDerivedMap(Map<TblColRef, DeriveInfo> derivedToHostMap) {
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

        for (Map.Entry<TblColRef, DeriveInfo> entry : derivedToHostMap.entrySet()) {

            TblColRef derCol = entry.getKey();
            TblColRef[] hostCols = entry.getValue().columns;
            DeriveInfo.DeriveType type = entry.getValue().type;
            JoinDesc join = entry.getValue().join;

            Array<TblColRef> hostColArray = new Array<>(hostCols);
            List<DeriveInfo> infoList = hostToDerivedMap.get(hostColArray);
            if (infoList == null) {
                infoList = new ArrayList<DeriveInfo>();
                hostToDerivedMap.put(hostColArray, infoList);
            }

            // Merged duplicated derived column
            boolean merged = false;
            for (DeriveInfo existing : infoList) {
                if (existing.type == type && existing.join.getPKSide().equals(join.getPKSide())) {
                    if (ArrayUtils.contains(existing.columns, derCol)) {
                        merged = true;
                        break;
                    }
                    if (type == DeriveInfo.DeriveType.LOOKUP) {
                        existing.columns = (TblColRef[]) ArrayUtils.add(existing.columns, derCol);
                        merged = true;
                        break;
                    }
                }
            }
            if (!merged)
                infoList.add(new DeriveInfo(type, join, new TblColRef[] { derCol }, false));
        }

        return hostToDerivedMap;
    }

    // load only needed dictionaries
    private Map<TblColRef, Dictionary<String>> buildDictionaryMap(List<TblColRef> columnsNeedDictionary) {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : columnsNeedDictionary) {
            result.put(col, dataSegment.getDictionary(col));
        }
        return result;
    }

    @Override
    public List<IAdvMeasureFiller> translateResult(Object[] gtValues, Tuple tuple) {
        assert gtValues.length == gtColIdx.length;

        // dimensions
        for (int i = 0; i < nSelectedDims; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0) {
                tuple.setDimensionValue(ti, toString(gtValues[i]));
            }
        }

        // measures
        for (int i = nSelectedDims; i < gtColIdx.length; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0 && measureTypes[i] != null) {
                measureTypes[i].fillTupleSimply(tuple, ti, gtValues[i]);
            }
        }

        // derived
        for (IDerivedColumnFiller filler : derivedColFillers) {
            filler.fillDerivedColumns(gtValues, tuple);
        }

        // advanced measure filling, due to possible row split, will complete at caller side
        if (advMeasureFillers.isEmpty()) {
            return null;
        } else {
            for (int i = 0; i < advMeasureFillers.size(); i++) {
                Object measureValue = gtValues[advMeasureIndexInGTValues.get(i)];
                advMeasureFillers.get(i).reload(measureValue);
            }
            return advMeasureFillers;
        }
    }

    private interface IDerivedColumnFiller {
        public void fillDerivedColumns(Object[] gtValues, Tuple tuple);
    }

    private IDerivedColumnFiller newDerivedColumnFiller(TblColRef[] hostCols, final DeriveInfo deriveInfo) {
        boolean allHostsPresent = true;
        final int[] hostTmpIdx = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostTmpIdx[i] = indexOnTheGTValues(hostCols[i]);
            allHostsPresent = allHostsPresent && hostTmpIdx[i] >= 0;
        }

        boolean needCopyDerived = false;
        final int[] derivedTupleIdx = new int[deriveInfo.columns.length];
        for (int i = 0; i < deriveInfo.columns.length; i++) {
            TblColRef col = deriveInfo.columns[i];
            derivedTupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            needCopyDerived = needCopyDerived || derivedTupleIdx[i] >= 0;
        }

        if (!(allHostsPresent && needCopyDerived))
            return null;

        switch (deriveInfo.type) {
        case LOOKUP:
            return new IDerivedColumnFiller() {
                LookupStringTable lookupTable = NDataflowManager
                        .getInstance(dataSegment.getConfig(), dataSegment.getProject())
                        .getLookupTable(dataSegment, deriveInfo.join);
                int[] derivedColIdx = initDerivedColIdx();
                Array<String> lookupKey = new Array<String>(new String[hostTmpIdx.length]);

                private int[] initDerivedColIdx() {
                    int[] idx = new int[deriveInfo.columns.length];
                    for (int i = 0; i < idx.length; i++) {
                        idx[i] = deriveInfo.columns[i].getColumnDesc().getZeroBasedIndex();
                    }
                    return idx;
                }

                @Override
                public void fillDerivedColumns(Object[] gtValues, Tuple tuple) {
                    for (int i = 0; i < hostTmpIdx.length; i++) {
                        lookupKey.data[i] = NCubeTupleConverter.toString(gtValues[hostTmpIdx[i]]);
                    }

                    String[] lookupRow = lookupTable.getRow(lookupKey);

                    if (lookupRow != null) {
                        for (int i = 0; i < derivedTupleIdx.length; i++) {
                            if (derivedTupleIdx[i] >= 0) {
                                String value = lookupRow[derivedColIdx[i]];
                                tuple.setDimensionValue(derivedTupleIdx[i], value);
                            }
                        }
                    } else {
                        for (int i = 0; i < derivedTupleIdx.length; i++) {
                            if (derivedTupleIdx[i] >= 0) {
                                tuple.setDimensionValue(derivedTupleIdx[i], null);
                            }
                        }
                    }
                }
            };
        case PK_FK:
            return new IDerivedColumnFiller() {
                @Override
                public void fillDerivedColumns(Object[] gtValues, Tuple tuple) {
                    // composite keys are split, so only copy [0] is enough, see CubeDesc.initDimensionColumns()
                    tuple.setDimensionValue(derivedTupleIdx[0], NCubeTupleConverter.toString(gtValues[hostTmpIdx[0]]));
                }
            };
        default:
            throw new IllegalArgumentException();
        }
    }

    private int indexOnTheGTValues(TblColRef col) {
        List<TblColRef> cuboidDims = cuboidLayout.getColumns();
        int cuboidIdx = cuboidDims.indexOf(col);
        for (int i = 0; i < gtColIdx.length; i++) {
            if (gtColIdx[i] == cuboidIdx)
                return i;
        }
        return -1;
    }

    private static String toString(Object o) {
        return o == null ? null : o.toString();
    }
}
