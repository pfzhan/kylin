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
package io.kyligence.kap.smart.query.advisor;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationCheck;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableDescManager;

public class CubeBasedSQLAdviceProposer extends AbstractSQLAdviceProposer {
    private RawTableDescManager rawTableDescManager;
    private CubeDesc cubeDesc;

    public CubeBasedSQLAdviceProposer(CubeDesc cubeDesc) {
        this.cubeDesc = cubeDesc;
        this.rawTableDescManager = RawTableDescManager.getInstance(cubeDesc.getConfig());
    }

    @Override
    public SQLAdvice propose(RealizationCheck.IncapableReason incapableReason, OLAPContext context) {
        switch (incapableReason.getIncapableType()) {
        case CUBE_NOT_READY:
            return SQLAdvice.build(String.format(msg.getCUBE_NOT_READY_REASON(), cubeDesc.getName()),
                    String.format(msg.getCUBE_NOT_READY_SUGGEST(), cubeDesc.getName()));
        case CUBE_NOT_CONTAIN_ALL_DIMENSION:
            String notFoundDimensionMsg = formatTblColRefs(incapableReason.getNotFoundDimensions());
            return SQLAdvice.build(
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_DIMENSIONS_REASON(), notFoundDimensionMsg,
                            cubeDesc.getName()),
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_DIMENSION_SUGGEST(), notFoundDimensionMsg,
                            cubeDesc.getName()));
        case CUBE_NOT_CONTAIN_ALL_MEASURE:
            String notFoundMeasureMsg = formatFunctionDescs(incapableReason.getNotFoundMeasures());
            return SQLAdvice.build(
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_MEASURES_REASON(), notFoundMeasureMsg,
                            cubeDesc.getName()),
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_MEASURES_SUGGEST(), notFoundMeasureMsg,
                            cubeDesc.getName()));
        case CUBE_NOT_CONTAIN_TABLE:
            String notContainTblMessage = formatTables(incapableReason.getNotFoundTables());
            return SQLAdvice.build(
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_TABLE_REASON(), notContainTblMessage, cubeDesc.getName()),
                    String.format(msg.getCUBE_NOT_CONTAIN_ALL_TABLE_SUGGEST(), notContainTblMessage,
                            cubeDesc.getName()));
        case MODEL_FACT_TABLE_NOT_FOUND:
            String tableName = context.firstTableScan.getOlapTable().getTableName();
            if (!cubeDesc.getModel().isLookupTable(tableName)) { // get pass if table snapshot can contribute
                return SQLAdvice.build(
                        String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_REASON(), tableName, cubeDesc.getModelName()),
                        String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_SUGGEST(), tableName, cubeDesc.getModelName()));
            } else {
                return null;
            }
        case CUBE_BLACK_OUT_REALIZATION:
            return SQLAdvice.build(String.format(msg.getCUBE_BLACK_OUT_REALIZATION_REASON(), cubeDesc.getName()),
                    msg.getDEFAULT_SUGGEST());
        case CUBE_UN_SUPPORT_MASSIN:
            return SQLAdvice.build(String.format(msg.getCUBE_UN_SUPPORT_MASSIN_REASON(), cubeDesc.getName()),
                    msg.getCUBE_UN_SUPPORT_MASSIN_SUGGEST());
        case CUBE_UN_SUPPORT_RAWQUERY:
            RawTableDesc rawTableDesc = rawTableDescManager.getRawTableDesc(cubeDesc.getName());
            if (rawTableDesc == null) { // get pass if raw_table can contribute
                return SQLAdvice.build(String.format(msg.getCUBE_UN_SUPPORT_RAWQUERY_REASON(), cubeDesc.getName()),
                        msg.getCUBE_UN_SUPPORT_RAWQUERY_SUGGEST());
            } else {
                return null;
            }
        case CUBE_UNMATCHED_DIMENSION:
            String dimensions = formatTblColRefs(incapableReason.getUnmatchedDimensions());
            return SQLAdvice.build(
                    String.format(msg.getCUBE_UNMATCHED_DIMENSIONS_REASON(), dimensions, cubeDesc.getName()),
                    String.format(msg.getCUBE_UNMATCHED_DIMENSIONS_SUGGEST(), dimensions, cubeDesc.getName()));
        case CUBE_LIMIT_PRECEDE_AGGR:
            return SQLAdvice.build(String.format(msg.getCUBE_LIMIT_PRECEDE_AGGR_REASON(), cubeDesc.getName()),
                    msg.getCUBE_LIMIT_PRECEDE_AGGR_SUGGEST());
        case CUBE_UNMATCHED_AGGREGATION:
            String aggregations = formatFunctionDescs(incapableReason.getUnmatchedAggregations());
            return SQLAdvice.build(
                    String.format(msg.getCUBE_UNMATCHED_AGGREGATIONS_REASON(), aggregations, cubeDesc.getName()),
                    String.format(msg.getCUBE_UNMATCHED_AGGREGATIONS_SUGGEST(), aggregations, cubeDesc.getName()));
        case CUBE_OTHER_CUBE_INCAPABLE:
            return SQLAdvice.build(msg.getCUBE_OTHER_CUBE_INCAPABLE_REASON(),
                    msg.getCUBE_OTHER_CUBE_INCAPABLE_SUGGEST());
        default:
            return null;
        }
    }
}
