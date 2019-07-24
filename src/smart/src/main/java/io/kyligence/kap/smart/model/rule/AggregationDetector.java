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

package io.kyligence.kap.smart.model.rule;

import java.util.List;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.Lists;

import lombok.Getter;

public class AggregationDetector extends SqlBasicVisitor<Object> {

    @Getter
    private List<String> blackList = Lists.newArrayList();

    @Getter
    private boolean effectiveAgg;

    @Override
    public Object visit(SqlCall call) {
        if (call.getOperator() instanceof SqlOverOperator) {
            // discard agg on window function
            blackList.add(call.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()).getSql());
            return null;
        } else if (call.getOperator() instanceof SqlAggFunction) {
            effectiveAgg = true;
            return null;
        } else if (call.getOperator() instanceof SqlAsOperator) {
            return call.getOperator().acceptCall(this, call);
        }
        return null;
    }
}
