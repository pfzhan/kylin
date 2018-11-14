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

package io.kyligence.kap.smart.model.cc;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlBasicVisitor;

@SuppressWarnings("rawtypes")
public class BlackListValidator extends SqlBasicVisitor {
    private boolean result = true;
    
    public boolean getResult() {
        return result;
    }

    @Override
    public Object visit(SqlIdentifier id) {
        if ("CURRENT_DATE".equalsIgnoreCase(id.toString()) || 
            "CURRENT_TIME".equalsIgnoreCase(id.toString()) || 
            "CURRENT_TIMESTAMP".equalsIgnoreCase(id.toString())) {
            result = false;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(SqlCall call) {
        SqlOperator op = call.getOperator();
        if (op instanceof SqlAggFunction) {
            result = false;
            return null;
        }
        if (op instanceof SqlJdbcFunctionCall) {
            if ("{fn CURRENT_DATE}".equalsIgnoreCase(op.getName()) || 
                "{fn CURRENT_TIME}".equalsIgnoreCase(op.getName()) || 
                "{fn CURRENT_TIMESTAMP}".equalsIgnoreCase(op.getName())) {
                result = false;
                return null;
            }
        }
        return call.getOperator().acceptCall(this, call);
    }
}
