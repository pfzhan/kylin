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

package io.kyligence.kap.modeling.smart.proposer;

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
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.util.Constants;
import io.kyligence.kap.modeling.smart.util.DimEncodingUtil;

public class RowkeyProposer extends AbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(RowkeyProposer.class);

    public RowkeyProposer(ModelingContext context) {
        super(context);
    }

    @Override
    public void doPropose(CubeDesc workCubeDesc) {
        updateRowKeyOrder(workCubeDesc);
        updateAttributes(workCubeDesc);
    }

    private double getRowkeyOrderVal(RowKeyColDesc r, boolean considerQuery) {
        // score = cardinality * DIM_ROWKEY_FILRER_PROMOTION_TIMES

        double score = 1;

        if (context.hasTableStats()) {
            TableExtDesc.ColumnStats s = context.getRowKeyColumnStats(r);

            if (s != null && context.getBusinessCoe() > 0) {
                score += s.getCardinality();
            }
        }

        if (considerQuery && context.hasQueryStats()) {
            String n = r.getColRef().getCanonicalName();

            Map<String, Integer> filters = context.getQueryStats().getFilters();
            Map<String, Integer> appears = context.getQueryStats().getAppears();

            if (filters.containsKey(n) && appears.containsKey(n)) {
                double filterScore1 = ((double) filters.get(n)) / ((double) appears.get(n));
                score *= filterScore1 * Constants.DIM_ROWKEY_FILRER_PROMOTION_TIMES;
            }
        }
        return score;
    }

    private void updateRowKeyOrder(CubeDesc workCubeDesc) {
        // sort rowkey order by cardinality desc
        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();

        logger.debug("Before sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));

        // split all rowkeys to 2 groups: filter, others
        List<RowKeyColDesc> filterRowkeys = Lists.newArrayList();
        List<RowKeyColDesc> otherRowkeys = Lists.newArrayList(rowKeyDescs);
        if (context.hasQueryStats()) {
            Map<String, Integer> filters = context.getQueryStats().getFilters();
            for (RowKeyColDesc filterCandidate : rowKeyDescs) {
                if (filters.containsKey(filterCandidate.getColRef().getCanonicalName())) {
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
        logger.debug("Sort filter rowkeys: {}", StringUtils.join(filterRowkeys, ","));

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
        logger.debug("Sort other rowkeys: {}", StringUtils.join(otherRowkeys, ","));

        List<RowKeyColDesc> allRowkeys = Lists.newArrayListWithExpectedSize(rowKeyDescs.length);
        allRowkeys.addAll(filterRowkeys);
        allRowkeys.addAll(otherRowkeys);
        rowKeyDescs = allRowkeys.toArray(rowKeyDescs);

        logger.debug("After sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));

    }

    private String selectDimEncoding(final RowKeyColDesc colDesc, final TableExtDesc.ColumnStats colStats) {
        if (colStats == null || colStats.getCardinality() < Constants.DIM_ENCODING_DICT_CARDINALITY_MAX) {
            // by default use dict for non-uhc cols
            return DictionaryDimEnc.ENCODING_NAME;
        }

        if (colDesc.getColRef().getType().isDate()) {
            return DateDimEnc.ENCODING_NAME;
        }

        int length = 0;

        if (colDesc.getColRef().getType().isIntegerFamily()) {
            // use integer for int family cols
            int maxValLength = DimEncodingUtil.getIntEncodingLength(Integer.parseInt(colStats.getMaxValue()));
            int minValLength = DimEncodingUtil.getIntEncodingLength(Integer.parseInt(colStats.getMinValue()));
            length = Math.max(maxValLength, minValLength);
            return String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, length);
        }

        // use fix_len for other cols
        length = Math.min(colStats.getMaxLengthValue().getBytes().length, Constants.DIM_ENCODING_FIXLEN_LENGTH_MAX);
        return String.format("%s:%d", FixedLenDimEnc.ENCODING_NAME, length);
    }

    private void updateAttributes(CubeDesc workCubeDesc) {
        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();

        // update dim encoding and shard by
        if (context.hasTableStats()) {
            RowKeyColDesc maxCardRowKey = null;
            long maxCardinality = 0;

            for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
                TableExtDesc.ColumnStats colStats = context.getRowKeyColumnStats(rowKeyDesc);
                long cardinality = colStats == null ? 0 : colStats.getCardinality();
                rowKeyDesc.setEncoding(selectDimEncoding(rowKeyDesc, colStats));
                logger.debug("Set dimension encoding: column={}, encoding={}, cardinality={}", rowKeyDesc.getColumn(), rowKeyDesc.getEncoding(), cardinality);

                if (cardinality > maxCardinality) {
                    // find the largest cardinality, set shardBy=true if the max exceeds threshold
                    maxCardinality = cardinality;
                    maxCardRowKey = rowKeyDesc;
                }
            }

            if (maxCardinality > Constants.DIM_UHC_MIN) {
                maxCardRowKey.setShardBy(true);
                logger.debug("Found shard by dimension: column={}, cardinality={}", maxCardRowKey.getColumn(), maxCardinality);
            }
        }

        // update index settings
        if (workCubeDesc.getEngineType() > IEngineAware.ID_SPARK || workCubeDesc.getStorageType() > IStorageAware.ID_SHARDED_HBASE) {
            final String indexSettings = "eq";
            // TODO: for LT and GT check, set all index
            for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
                rowKeyDesc.setIndex(indexSettings);
            }
        }
    }
}
