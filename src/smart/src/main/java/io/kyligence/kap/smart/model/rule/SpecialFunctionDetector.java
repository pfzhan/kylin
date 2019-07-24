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

import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.common.SmartConfig;
import lombok.Getter;
import lombok.val;

public class SpecialFunctionDetector extends SqlBasicVisitor<Object> {

    // configurable
    private static ImmutableSet<SqlKind> specialSqlKinds = ImmutableSet.copyOf(initSpecialSqlKind());

    @Getter
    private boolean valid = false;

    @Override
    public Object visit(SqlCall call) {
        final val operator = call.getOperator();
        if (operator instanceof SqlFunction && specialSqlKinds.contains(operator.getKind())) {
            if (!isLiteralCall(call)) {
                valid = true;
            }
            return null;
        }
        return call.getOperator().acceptCall(this, call);
    }

    private boolean isLiteralCall(SqlCall call) {
        LiteralCallVisitor visitor = new LiteralCallVisitor();
        call.accept(visitor);
        return visitor.isLiteralCall();
    }

    // if there is SqlIdentifier, it's not literal sql call
    private static class LiteralCallVisitor extends SqlBasicVisitor<Object> {

        @Getter
        private boolean literalCall = true;

        @Override
        public Object visit(SqlIdentifier id) {
            literalCall = false;
            return null;
        }
    }

    private static Set<SqlKind> initSpecialSqlKind() {
        SmartConfig config = SmartConfig.getInstanceFromEnv();
        String[] specialFunctions = config.getFunctionsAppliedToCCRules();

        Set<SqlKind> rst = Sets.newHashSet();
        for (String function : specialFunctions) {
            rst.add(SqlKind.valueOf(function));
        }

        return rst;
    }

}
