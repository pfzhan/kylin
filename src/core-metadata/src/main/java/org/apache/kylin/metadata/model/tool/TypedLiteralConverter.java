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

package org.apache.kylin.metadata.model.tool;

import java.math.BigDecimal;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.kylin.metadata.model.DataType;

public class TypedLiteralConverter {
    private static final String NULL = "KY_LITERAL_NULL";

    public static Object stringValueToTypedValue(String value, DataType dataType) {
        if (value.equals(NULL)) {
            return null;
        }
        switch (dataType.getTypeName()) {
        case CHAR:
        case VARCHAR:
            return value;
        case DECIMAL:
            return new BigDecimal(value);
        case BIGINT:
            return Long.parseLong(value);
        case SMALLINT:
            return Short.parseShort(value);
        case TINYINT:
            return Byte.parseByte(value);
        case INTEGER:
            return Integer.parseInt(value);
        case DOUBLE:
            return Double.parseDouble(value);
        case FLOAT:
        case REAL:
            return Float.parseFloat(value);
        case DATE:
            return DateString.fromDaysSinceEpoch(Integer.parseInt(value));
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            return TimeString.fromMillisOfDay(Integer.parseInt(value));
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return TimestampString.fromMillisSinceEpoch(Long.parseLong(value));
        case BOOLEAN:
            return Boolean.parseBoolean(value);
        default:
            return value;
        }
    }

    public static String typedLiteralToString(RexLiteral literal) {
        if (literal.getValue3() == null) {
            return NULL;
        }
        return String.valueOf(literal.getValue3());
    }
}