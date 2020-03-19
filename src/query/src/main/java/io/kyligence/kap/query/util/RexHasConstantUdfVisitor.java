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
package io.kyligence.kap.query.util;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import java.util.Optional;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * In constant query,
 * visit RexNode to find if any udf implement {@link org.apache.calcite.sql.type.NotConstant}
 */
public class RexHasConstantUdfVisitor extends RexVisitorImpl<Boolean> {

    public RexHasConstantUdfVisitor() {
        super(true);
    }

    @Override
    public Boolean visitCall(RexCall call) {
        if ((call.getOperator() instanceof SqlUserDefinedFunction)
                && (((SqlUserDefinedFunction) (call.getOperator())).getFunction() instanceof ScalarFunctionImpl)) {

            SqlUserDefinedFunction sqlUserDefinedFunction = (SqlUserDefinedFunction) (call.getOperator());

            ScalarFunctionImpl scalarFunction = (ScalarFunctionImpl) (sqlUserDefinedFunction.getFunction());

            if (NotConstant.class.isAssignableFrom(scalarFunction.method.getDeclaringClass())) {
                return TRUE;
            }
        }

        return call.getOperands().stream().anyMatch(operand -> defaultForEmpty(operand.accept(this)));
    }

    private static Boolean defaultForEmpty(Boolean result) {
        return Optional.ofNullable(result).orElse(FALSE);

    }

}
