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

package io.kyligence.kap.smart.cube.proposer;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.dimension.TimeDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.query.Utils;

public class RowkeyProposer extends AbstractCubeProposer {
    private static final Logger logger = LoggerFactory.getLogger(RowkeyProposer.class);

    public RowkeyProposer(CubeContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        int rowkeySize = workCubeDesc.getRowkey().getRowKeyColumns().length;
//        Preconditions.checkState(rowkeySize < 64, String.format(
//                "Too many rowkeys (%s) in CubeDesc, please try to reduce dimension number or adopt derived dimensions",
//                rowkeySize));

        Utils.removeLargeRowkeySizeConf(workCubeDesc.getOverrideKylinProps());
        updateRowKeyOrder(workCubeDesc);
        updateAttributes(workCubeDesc);
    }

    private double getRowkeyOrderVal(RowKeyColDesc r, boolean considerQuery) {
        // score = cardinality * DIM_ROWKEY_FILRER_PROMOTION_TIMES
        double score = 1;

        if ((context.hasModelStats() || context.hasTableStats()) && smartConfig.getPhyscalWeight() > 0) {
            long cardinality = context.getColumnsCardinality(r.getColRef().getIdentity());
            if (cardinality > 0) {
                score += cardinality;
            }
        }

        if (considerQuery && context.hasQueryStats()) {
            String n = r.getColRef().getIdentity();

            Map<String, Integer> filters = context.getQueryStats().getFilters();
            Map<String, Integer> appears = context.getQueryStats().getAppears();

            if (filters.containsKey(n) && appears.containsKey(n)) {
                double filterScore1 = ((double) filters.get(n)) / ((double) appears.get(n));
                score *= filterScore1 * smartConfig.getRowkeyFilterPromotionTimes();
            }
        }
        return score;
    }

    private void updateRowKeyOrder(CubeDesc workCubeDesc) {
        // sort rowkey order by cardinality desc
        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();

        logger.trace("Before sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));

        // split all rowkeys to 2 groups: filter, others
        List<RowKeyColDesc> filterRowkeys = Lists.newArrayList();
        List<RowKeyColDesc> otherRowkeys = Lists.newArrayList(rowKeyDescs);
        if (context.hasQueryStats()) {
            Map<String, Integer> filters = context.getQueryStats().getFilters();
            for (RowKeyColDesc filterCandidate : rowKeyDescs) {
                Integer filterCnt = filters.get(filterCandidate.getColRef().getIdentity());
                if (filterCnt != null && filterCnt > 0) {
                    filterRowkeys.add(filterCandidate);
                }
            }
        }
        otherRowkeys.removeAll(filterRowkeys);

        // sort filter with filter ratios
        final Map<RowKeyColDesc, Double> filterRowkeyVals = Maps.newHashMap();
        for (RowKeyColDesc rowKeyColDesc : filterRowkeys) {
            filterRowkeyVals.put(rowKeyColDesc, getRowkeyOrderVal(rowKeyColDesc, true));
        }
        Collections.sort(filterRowkeys, Collections.reverseOrder(new Comparator<RowKeyColDesc>() {
            @Override
            public int compare(RowKeyColDesc r1, RowKeyColDesc r2) {
                return (int) (filterRowkeyVals.get(r1) - filterRowkeyVals.get(r2));
            }
        }));
        logger.trace("Sort filter rowkeys: {}", StringUtils.join(filterRowkeys, ","));

        // sort others with cardinalites
        final Map<RowKeyColDesc, Double> otherRowkeyVals = Maps.newHashMap();
        for (RowKeyColDesc rowKeyColDesc : otherRowkeys) {
            otherRowkeyVals.put(rowKeyColDesc, getRowkeyOrderVal(rowKeyColDesc, false));
        }
        Collections.sort(otherRowkeys, Collections.reverseOrder(new Comparator<RowKeyColDesc>() {
            @Override
            public int compare(RowKeyColDesc r1, RowKeyColDesc r2) {
                return (int) (otherRowkeyVals.get(r1) - otherRowkeyVals.get(r2));
            }
        }));
        logger.trace("Sort other rowkeys: {}", StringUtils.join(otherRowkeys, ","));

        List<RowKeyColDesc> allRowkeys = Lists.newArrayListWithExpectedSize(rowKeyDescs.length);
        allRowkeys.addAll(filterRowkeys);
        allRowkeys.addAll(otherRowkeys);
        rowKeyDescs = allRowkeys.toArray(rowKeyDescs);

        logger.trace("After sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));
    }

    private String selectDimEncoding(final RowKeyColDesc colDesc, final long cardinality) {
        // select encoding according to column type
        // eg. date, tinyint, integer, smallint, bigint
        // TODO: we can set boolean encoding according to column type and cardinality, but cardinality is not precise.
        DataType dataType = colDesc.getColRef().getType();

        // datatime family
        if (dataType.isDate()) {
            return DateDimEnc.ENCODING_NAME;
        } else if (dataType.isDateTimeFamily()) {
            return TimeDimEnc.ENCODING_NAME;
        }

        // number family
        if (dataType.isTinyInt()) {
            return String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 1);
        } else if (dataType.isSmallInt()) {
            return String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 2);
        } else if (dataType.isInt()) {
            return String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 4);
        } else if (dataType.isIntegerFamily()) {
            return String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 8);
        } else if (dataType.isNumberFamily()) {
            return DictionaryDimEnc.ENCODING_NAME;
        }

        // select dict or fixlen for other type columns according to cardinality
        if (context.hasTableStats() && cardinality > smartConfig.getRowkeyDictEncCardinalityMax()) {
            TableExtDesc.ColumnStats colStats = context.getTableColumnStats(colDesc.getColRef());
            // TODO: currently used max length, better to use 95%ile length
            int length = Math.min(colStats.getMaxLengthValue().getBytes().length,
                    smartConfig.getRowkeyFixLenLengthMax());
            return String.format("%s:%d", FixedLenDimEnc.ENCODING_NAME, length);
        }

        // if cardinality is not high, then use dict
        if (cardinality <= smartConfig.getRowkeyDictEncCardinalityMax()) {
            return DictionaryDimEnc.ENCODING_NAME;
        }

        // finally, keep non-default encoding for special cases
        String defaultEnc = smartConfig.getRowkeyDefaultEnc();
        if (!colDesc.getEncoding().equals(defaultEnc)) {
            return colDesc.getEncoding();
        }

        return defaultEnc;
    }

    private void updateAttributes(CubeDesc workCubeDesc) {
        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();

        // update dim encoding and shard by
        RowKeyColDesc maxCardRowKey = null;
        long maxCardinality = 0;

        for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
            long cardinality = context.getColumnsCardinality(rowKeyDesc.getColRef().getIdentity());
            if (cardinality < 0) {
                continue; // skip columns without cardinality information
            }
            rowKeyDesc.setEncoding(selectDimEncoding(rowKeyDesc, cardinality));
            logger.trace("Set dimension encoding: column={}, encoding={}, cardinality={}", rowKeyDesc.getColumn(),
                    rowKeyDesc.getEncoding(), cardinality);

            if (cardinality > maxCardinality) {
                // find the largest cardinality, set shardBy=true if the max exceeds threshold
                maxCardinality = cardinality;
                maxCardRowKey = rowKeyDesc;
            }
        }

        if (maxCardRowKey != null && maxCardinality > smartConfig.getRowkeyUHCCardinalityMin()) {
            maxCardRowKey.setShardBy(true);
            logger.trace("Found shard by dimension: column={}, cardinality={}", maxCardRowKey.getColumn(),
                    maxCardinality);
        }

        // update index settings
        if (workCubeDesc.getEngineType() > IEngineAware.ID_SPARK
                || workCubeDesc.getStorageType() > IStorageAware.ID_SHARDED_HBASE) {
            final String indexSettings = "eq";
            // TODO: for LT and GT check (eg. data column), set all index
            for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
                rowKeyDesc.setIndex(indexSettings);
            }
        }
    }
}
