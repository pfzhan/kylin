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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.query.QueryStats;
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

    private int compareRowkeyColDescs(RowKeyColDesc r1, RowKeyColDesc r2) {
        // score = cardinality * DIM_ROWKEY_FILRER_PROMOTION_TIMES
        TableExtDesc.ColumnStats s1 = context.getRowKeyColumnStats(r1);
        TableExtDesc.ColumnStats s2 = context.getRowKeyColumnStats(r2);
        double score1 = 1;
        double score2 = 1;
        if (s1 != null && s2 != null && context.getBusinessCoe() > 0) {
            score1 += s1.getCardinality();
            score2 += s2.getCardinality();
        }

        QueryStats queryStats = context.getQueryStats();
        if (queryStats != null) {
            Map<String, Integer> filters = queryStats.getFilters();
            Integer filter1 = filters.get(r1.getColRef().getCanonicalName());
            Integer filter2 = filters.get(r2.getColRef().getCanonicalName());

            if (filter1 == null) {
                filter1 = 0;
            }
            if (filter2 == null) {
                filter2 = 0;
            }

            if (filter1 > filter2) {
                score1 *= Constants.DIM_ROWKEY_FILRER_PROMOTION_TIMES;
            } else {
                score2 *= Constants.DIM_ROWKEY_FILRER_PROMOTION_TIMES;
            }
        }

        return (int) (score1 - score2);
    }

    private void updateRowKeyOrder(CubeDesc workCubeDesc) {
        // sort rowkey order by cardinality desc
        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();
        logger.debug("Before sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("Before - Cardinality: ");
            for (RowKeyColDesc rowKeyColDesc : rowKeyDescs) {
                sb.append(context.getRowKeyColumnStats(rowKeyColDesc).getCardinality());
                sb.append(",");
            }
            logger.debug(sb.toString());
        }
        Arrays.sort(rowKeyDescs, Collections.reverseOrder(new Comparator<RowKeyColDesc>() {
            @Override
            public int compare(RowKeyColDesc r1, RowKeyColDesc r2) {
                return compareRowkeyColDescs(r1, r2);
            }
        }));
        logger.debug("After sorting row keys: {}", StringUtils.join(rowKeyDescs, ","));
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("After - Cardinality: ");
            for (RowKeyColDesc rowKeyColDesc : rowKeyDescs) {
                sb.append(context.getRowKeyColumnStats(rowKeyColDesc).getCardinality());
                sb.append(",");
            }
            logger.debug(sb.toString());
        }
    }

    private String selectDimEncoding(final RowKeyColDesc colDesc, final TableExtDesc.ColumnStats colStats) {
        if (colStats.getCardinality() < Constants.DIM_ENCODING_DICT_CARDINALITY_MAX) {
            // by default use dict for non-uhc cols
            return "dict";
        }

        int length = 0;
        if (colDesc.getColRef().getType().isIntegerFamily()) {
            // use integer for int family cols
            int maxValLength = DimEncodingUtil.getIntEncodingLength(Integer.parseInt(colStats.getMaxValue()));
            int minValLength = DimEncodingUtil.getIntEncodingLength(Integer.parseInt(colStats.getMinValue()));
            length = Math.max(maxValLength, minValLength);
            return String.format("integer:%d", length);
        }

        // use fix_len for other cols
        length = Math.min(colStats.getMaxLengthValue().getBytes().length, Constants.DIM_ENCODING_FIXLEN_LENGTH_MAX);
        return String.format("fix_length:%d", length);
    }

    private void updateAttributes(CubeDesc workCubeDesc) {
        RowKeyColDesc maxCardRowKey = null;
        long maxCardinality = 0;

        RowKeyColDesc[] rowKeyDescs = workCubeDesc.getRowkey().getRowKeyColumns();
        for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
            TableExtDesc.ColumnStats colStats = context.getRowKeyColumnStats(rowKeyDesc);
            rowKeyDesc.setEncoding(selectDimEncoding(rowKeyDesc, colStats));
            logger.debug("Set dimension encoding: column={}, encoding={}, cardinality={}", rowKeyDesc.getColumn(), rowKeyDesc.getEncoding(), colStats.getCardinality());

            if (colStats.getCardinality() > maxCardinality) {
                // find the largest cardinality, set shardBy=true if the max exceeds threshold
                maxCardinality = colStats.getCardinality();
                maxCardRowKey = rowKeyDesc;
            }
        }

        if (maxCardinality > Constants.DIM_UHC_MIN) {
            maxCardRowKey.setShardBy(true);
            logger.debug("Found shard by dimension: column={}, cardinality={}", maxCardRowKey.getColumn(), maxCardinality);
        }

        // update index settings
        if (workCubeDesc.getEngineType() != 2 || workCubeDesc.getStorageType() != 2) {
            final String indexSettings = "onlyEq";
            for (RowKeyColDesc rowKeyDesc : rowKeyDescs) {
                rowKeyDesc.setIndex(indexSettings);
            }
        }
    }
}
