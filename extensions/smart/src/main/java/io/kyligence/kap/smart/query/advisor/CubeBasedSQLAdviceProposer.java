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
        SQLAdvice sqlAdvice = new SQLAdvice();
        switch (incapableReason.getIncapableType()) {
        case CUBE_NOT_READY:
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_NOT_READY_REASON(), cubeDesc.getName()));
            sqlAdvice.setSuggestion(String.format(msg.getCUBE_NOT_READY_SUGGEST(), cubeDesc.getName()));
            break;
        case CUBE_NOT_CONTAIN_ALL_DIMENSION:
            String notFoundDimensionMsg = formatTblColRefs(incapableReason.getNotFoundDimensions());
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_NOT_CONTAIN_ALL_DIMENSION_REASON(),
                    notFoundDimensionMsg, cubeDesc.getName()));
            sqlAdvice.setSuggestion(String.format(msg.getCUBE_NOT_CONTAIN_ALL_DIMENSION_SUGGEST(), notFoundDimensionMsg,
                    cubeDesc.getName()));
            break;
        case CUBE_NOT_CONTAIN_ALL_MEASURE:
            String notFoundMeasureMsg = formatTblColRefs(incapableReason.getNotFoundMeasures());
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_NOT_CONTAIN_ALL_MEASURE_REASON(), notFoundMeasureMsg,
                    cubeDesc.getName()));
            sqlAdvice.setSuggestion(String.format(msg.getCUBE_NOT_CONTAIN_ALL_MEASURE_SUGGEST(), notFoundMeasureMsg,
                    cubeDesc.getName()));
            break;
        case CUBE_NOT_CONTAIN_TABLE:
            String notContainTblMessage = formatTables(incapableReason.getNotFoundTables());
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_NOT_CONTAIN_ALL_TABLE_REASON(), notContainTblMessage,
                    cubeDesc.getName()));
            sqlAdvice.setSuggestion(String.format(msg.getCUBE_NOT_CONTAIN_ALL_TABLE_SUGGEST(), notContainTblMessage,
                    cubeDesc.getName()));
            break;
        case MODEL_FACT_TABLE_NOT_FOUND:
            String tableName = context.firstTableScan.getOlapTable().getTableName();
            sqlAdvice.setIncapableReason(
                    String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_REASON(), tableName, cubeDesc.getModelName()));
            sqlAdvice.setSuggestion(
                    String.format(msg.getMODEL_FACT_TABLE_UNMATCHED_SUGGEST(), tableName, cubeDesc.getModelName()));
            break;
        case CUBE_BLACK_OUT_REALIZATION:
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_BLACK_OUT_REALIZATION_REASON(), cubeDesc.getName()));
            sqlAdvice.setSuggestion(msg.getDEFAULT_SUGGEST());
            break;
        case CUBE_UN_SUPPORT_MASSIN:
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_UN_SUPPORT_MASSIN_REASON(), cubeDesc.getName()));
            sqlAdvice.setSuggestion(msg.getCUBE_UN_SUPPORT_MASSIN_SUGGEST());
            break;
        case CUBE_UN_SUPPORT_RAWQUERY:
            RawTableDesc rawTableDesc = rawTableDescManager.getRawTableDesc(cubeDesc.getName());
            if (rawTableDesc != null) {
                return null;
            }
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_UN_SUPPORT_RAWQUERY_REASON(), cubeDesc.getName()));
            sqlAdvice.setSuggestion(msg.getCUBE_UN_SUPPORT_RAWQUERY_SUGGEST());
            break;
        case CUBE_UNMATCHED_DIMENSION:
            String dimensions = formatTblColRefs(incapableReason.getUnmatchedDimensions());
            sqlAdvice.setIncapableReason(
                    String.format(msg.getCUBE_UNMATCHED_DIMENSION_REASON(), dimensions, cubeDesc.getName()));
            sqlAdvice.setSuggestion(
                    String.format(msg.getCUBE_UNMATCHED_DIMENSION_SUGGEST(), dimensions, cubeDesc.getName()));
            break;
        case CUBE_LIMIT_PRECEDE_AGGR:
            sqlAdvice.setIncapableReason(String.format(msg.getCUBE_LIMIT_PRECEDE_AGGR_REASON(), cubeDesc.getName()));
            sqlAdvice.setSuggestion(msg.getCUBE_LIMIT_PRECEDE_AGGR_SUGGEST());
            break;
        case CUBE_UNMATCHED_AGGREGATION:
            String aggregations = formatFunctionDescs(incapableReason.getUnmatchedAggregations());
            sqlAdvice.setIncapableReason(
                    String.format(msg.getCUBE_UNMATCHED_AGGREGATION_REASON(), aggregations, cubeDesc.getName()));
            sqlAdvice.setSuggestion(
                    String.format(msg.getCUBE_UNMATCHED_AGGREGATION_SUGGEST(), aggregations, cubeDesc.getName()));
            break;
        case CUBE_OTHER_CUBE_INCAPABLE:
            sqlAdvice.setIncapableReason(msg.getCUBE_OTHER_CUBE_INCAPABLE_REASON());
            sqlAdvice.setSuggestion(msg.getCUBE_OTHER_CUBE_INCAPABLE_SUGGEST());
            break;
        default:
            break;
        }
        return sqlAdvice;
    }
}
