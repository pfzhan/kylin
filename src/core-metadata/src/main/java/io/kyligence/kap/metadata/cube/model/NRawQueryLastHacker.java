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
package io.kyligence.kap.metadata.cube.model;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRawQueryLastHacker {

    private static final Logger logger = LoggerFactory.getLogger(NRawQueryLastHacker.class);

    public static void hackNoAggregations(SQLDigest sqlDigest, NDataflow nDataflow) {
        if (!sqlDigest.isRawQuery || BackdoorToggles.getDisabledRawQueryLastHacker()) {
            return;
        }

        // If no group by and metric found, then it's simple query like select ... from ... where ...,
        // But we have no raw data stored, in order to return better results, we hack to output sum of metric column
        logger.info(
                "No group by and aggregation found in this query, will hack some result for better look of output...");

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.

        // @TODO refactor select-star judge
        for (TblColRef col : nDataflow.getAllColumns()) {
            if (nDataflow.getIndexPlan().listDimensionColumnsExcludingDerived(null).contains(col)) {
                sqlDigest.allColumns.add(col);
            }
        }

        for (TblColRef col : sqlDigest.allColumns) {
            if (nDataflow.getIndexPlan().listDimensionColumnsExcludingDerived(null).contains(col)) {
                // For dimension columns, take them as group by columns.
                sqlDigest.groupbyColumns.add(col);
            } else {
                // For measure columns, take them as metric columns with aggregation function SUM().
                ParameterDesc parameter = ParameterDesc.newInstance(col);
                FunctionDesc sumFunc = FunctionDesc.newInstance("SUM", parameter, null);

                boolean measureHasSum = false;
                for (MeasureDesc colMeasureDesc : nDataflow.getMeasures()) {
                    if (colMeasureDesc.getFunction().equals(sumFunc)) {
                        measureHasSum = true;
                        break;
                    }
                }

                if (measureHasSum) {
                    sqlDigest.aggregations.add(sumFunc);
                } else {
                    logger.warn("SUM is not defined for measure column " + col + ", output will be meaningless.");
                }

                sqlDigest.metricColumns.add(col);
            }
        }
    }
}
