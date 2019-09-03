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

package org.apache.kylin.metadata.model;

import java.io.Serializable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.kylin.metadata.model.tool.NonEquiJoinConditionComparator;
import org.apache.kylin.metadata.model.tool.TypedLiteralConverter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NonEquiJoinCondition implements Serializable {

    @Getter
    @Setter
    @JsonProperty("type")
    private NonEquiJoinConditionType type;

    @Getter
    @Setter
    @JsonProperty("data_type")
    private DataType dataType; // data type of the corresponding rex node

    @Getter
    @Setter
    @JsonProperty("op")
    private SqlKind op; // kind of the operator

    @Getter
    @Setter
    @JsonProperty("op_name")
    private String opName; // name of the operator

    @Getter
    @Setter
    @JsonProperty("operands")
    private NonEquiJoinCondition[] operands = new NonEquiJoinCondition[0]; // nested operands

    @Getter
    @Setter
    @JsonProperty("value")
    private String value; // literal or column identity at leaf node

    @Getter
    @Setter
    private TblColRef colRef; // set at runtime with model init

    @Getter
    @Setter
    @JsonProperty("expr")
    private String expr;

    public NonEquiJoinCondition() {
    }

    public NonEquiJoinCondition(SqlOperator op, NonEquiJoinCondition[] operands, RelDataType dataType) {
        this(op.getName(), op.getKind(), operands, new DataType(dataType));
    }

    public NonEquiJoinCondition(RexLiteral value, RelDataType dataType) {
        this(TypedLiteralConverter.typedLiteralToString(value), new DataType(dataType));
    }

    public NonEquiJoinCondition(TblColRef tblColRef, RelDataType dataType) {
        this(tblColRef, new DataType(dataType));
    }

    public NonEquiJoinCondition(String opName, SqlKind op, NonEquiJoinCondition[] operands, DataType dataType) {
        this.opName = opName;
        this.op = op;
        this.operands = operands;
        this.type = NonEquiJoinConditionType.EXPRESSION;
        this.dataType = dataType;
    }

    public NonEquiJoinCondition(String value, DataType dataType) {
        this.op = SqlKind.LITERAL;
        this.type = NonEquiJoinConditionType.LITERAL;
        this.value = value;
        this.dataType = dataType;
    }

    public NonEquiJoinCondition(TblColRef tblColRef, DataType dataType) {
        this.op = SqlKind.INPUT_REF;
        this.type = NonEquiJoinConditionType.COLUMN;
        this.value = tblColRef.getIdentity();
        this.colRef = tblColRef;
        this.dataType = dataType;
    }

    public Object getTypedValue() {
        return TypedLiteralConverter.stringValueToTypedValue(value, dataType);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(op);
        sb.append("(");
        for (NonEquiJoinCondition input : operands) {
            sb.append(input.toString());
            sb.append(", ");
        }
        if (value != null) {
            sb.append(value);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null &&
                obj instanceof NonEquiJoinCondition &&
                NonEquiJoinConditionComparator.equals(this, (NonEquiJoinCondition) obj);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + type.hashCode();
        result = prime * result + dataType.hashCode();
        result = prime * result + op.hashCode();
        if (opName != null) {
            result = prime * result + opName.hashCode();
        }
        for (NonEquiJoinCondition operand : operands) {
            result = prime * result + operand.hashCode();
        }
        result = prime * result + value.hashCode();
        return result;
    }
}
