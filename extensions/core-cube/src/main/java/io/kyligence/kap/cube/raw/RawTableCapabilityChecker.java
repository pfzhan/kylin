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

package io.kyligence.kap.cube.raw;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cube.JoinChecker;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class RawTableCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(RawTableCapabilityChecker.class);

    public static CapabilityResult check(RawTableInstance rawTable, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // match joins
        boolean isJoinMatch = JoinChecker.isJoinMatch(digest.joinDescs, rawTable);
        if (!isJoinMatch) {
            logger.info("Exclude RawTableInstance " + rawTable.getName() + " because unmatched joins");
            return result;
        }

        //raw table cannot handle lookup queries
        if (!StringUtils.equals(digest.factTable, rawTable.getRootFactTable())) {
            logger.info("Exclude RawTableInstance " + rawTable.getName() + " because the query does not contain fact table");
            return result;
        }

        Collection<TblColRef> missingColumns = Sets.newHashSet(digest.allColumns);
        missingColumns.removeAll(rawTable.getAllColumns());
        if (missingColumns.size() > 0) {
            logger.info("Exclude rawtable " + rawTable.getName() + " because missing column(s):" + missingColumns);
        }

        //handle distinct count by kylin, cuz calcite can't
        Iterator<FunctionDesc> it = digest.aggregations.iterator();
        while (it.hasNext()) {

            FunctionDesc functionDesc = it.next();
            // let calcite handle count
            if (functionDesc.isCount()) {
                continue;
            }

            ParameterDesc parameterDesc = functionDesc.getParameter();
            if (parameterDesc != null) {
                List<TblColRef> neededCols = parameterDesc.getColRefs();
                if (neededCols.size() > 0 && rawTable.getRawTableDesc().getColumns().containsAll(neededCols) && FunctionDesc.FUNC_COUNT_DISTINCT.equals(functionDesc.getExpression())) {
                    result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                    //                    functionDesc.setDimensionAsMetric(true);
                    //                    logger.info("Adjust DimensionAsMeasure for " + functionDesc);
                }
            }
        }

        // cost will be minded by caller
        result.capable = true;
        return result;
    }

}
