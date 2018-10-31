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

package io.kyligence.kap.metadata.acl;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE //
        , getterVisibility = JsonAutoDetect.Visibility.NONE //
        , isGetterVisibility = JsonAutoDetect.Visibility.NONE //
        , setterVisibility = JsonAutoDetect.Visibility.NONE) //
//all row conds in the table, for example:C1:{cond1, cond2},C2{cond1, cond3}, immutable
public class ColumnToConds extends CaseInsensitiveStringMap<List<ColumnToConds.Cond>> implements Serializable, IKeep {

    public ColumnToConds() {
    }

    public ColumnToConds(Map<String, List<Cond>> columnToConds) {
        super.putAll(columnToConds);
    }

    public List<Cond> getCondsByColumn(String col) {
        List<Cond> conds = super.get(col);
        if (conds == null) {
            conds = new ArrayList<>();
        }
        return ImmutableList.copyOf(conds);
    }

    @Override
    public Set<String> keySet() {
        return ImmutableSet.copyOf(super.keySet());
    }

    static Map<String, String> getColumnWithType(String project, String table) {
        Map<String, String> columnWithType = new HashMap<>();
        ColumnDesc[] columns = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                .getTableDesc(table) //
                .getColumns(); //
        for (ColumnDesc column : columns) {
            columnWithType.put(column.getName(), column.getTypeName());
        }
        return columnWithType;
    }

    static String concatConds(ColumnToConds condsWithCol, Map<String, String> columnWithType) {
        StrBuilder result = new StrBuilder();
        int j = 0;
        for (String col : condsWithCol.keySet()) {
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type not found");
            List<Cond> conds = condsWithCol.getCondsByColumn(col);
            for (int i = 0; i < conds.size(); i++) {
                String parsedCond = conds.get(i).toString(col, type);
                if (conds.size() == 1) {
                    result.append(parsedCond);
                    continue;
                }
                if (i == 0) {
                    result.append("(").append(parsedCond).append(" OR ");
                    continue;
                }
                if (i == conds.size() - 1) {
                    result.append(parsedCond).append(")");
                    continue;
                }
                result.append(parsedCond).append(" OR ");
            }
            if (j != condsWithCol.size() - 1) {
                result.append(" AND ");
            }
            j++;
        }
        return result.toString();
    }

    public static String preview(String project, String table, ColumnToConds condsWithColumn) throws IOException {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return concatConds(condsWithColumn, columnWithType);
    }

    @JsonSerialize(using = Cond.RowACLCondSerializer.class)
    @JsonDeserialize(using = Cond.RowACLCondDeserializer.class)
    public static class Cond implements Serializable, IKeep {
        public enum IntervalType implements Serializable, IKeep {
            OPEN, //(a,b) = {x | a < x < b}
            CLOSED, //[a,b] = {x | a <= x <= b}
            LEFT_INCLUSIVE, //[a,b) = {x | a <= x < b}
            RIGHT_INCLUSIVE, //(a,b] = {x | a < x <= b}
        }

        private IntervalType type;
        private String leftExpr;
        private String rightExpr;

        //just for json deserialization
        Cond() {
        }

        Cond(IntervalType type, String leftExpr, String rightExpr) {
            this.type = type;
            this.leftExpr = leftExpr;
            this.rightExpr = rightExpr;
        }

        public Cond(String value) {
            this.type = IntervalType.CLOSED;
            this.leftExpr = this.rightExpr = value;
        }

        String toString(String column, String columnType) {
            Pair<String, String> op = getOp(type);
            String leftValue = trim(leftExpr, columnType);
            String rightValue = trim(rightExpr, columnType);

            if (leftValue == null && rightValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<" + rightValue + ")";
                } else if (type == IntervalType.RIGHT_INCLUSIVE) {
                    return "(" + column + "<=" + rightValue + ")";
                } else {
                    throw new RuntimeException("error expr");
                }
            }

            if (rightValue == null && leftValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + ">" + leftValue + ")";
                } else if (type == IntervalType.LEFT_INCLUSIVE) {
                    return "(" + column + ">=" + leftValue + ")";
                } else {
                    throw new RuntimeException("error expr");
                }
            }

            if ((leftValue == null && rightValue == null) || leftValue.equals(rightValue)) {
                if (type == IntervalType.CLOSED) {
                    return "(" + column + "=" + leftValue + ")";
                }
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<>" + leftValue + ")";
                }
            }
            return "(" + column + op.getFirst() + leftValue + " AND " + column + op.getSecond() + rightValue + ")";
        }

        //add cond with single quote and escape single quote
        static String trim(String expr, String type) {
            if (expr == null) {
                return null;
            }
            if (type.startsWith("varchar") || type.equals("string") || type.equals("char")) {
                expr = expr.replaceAll("'", "''");
                expr = "'" + expr + "'";
            }
            if (type.equals("date")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                expr = "DATE '" + expr + "'";
            }
            if (type.equals("timestamp") || type.equals("datetime")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                expr = "TIMESTAMP '" + expr + "'";
            }
            if (type.equals("time")) {
                final int TIME_START_POS = 11; //"1970-01-01 ".length() = 11
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                //transform "1970-01-01 00:00:59" into "00:00:59"
                expr = "TIME '" + expr.substring(TIME_START_POS, expr.length()) + "'";
            }
            return expr;
        }

        private static Pair<String, String> getOp(Cond.IntervalType type) {
            switch (type) {
            case OPEN:
                return Pair.newPair(">", "<");
            case CLOSED:
                return Pair.newPair(">=", "<=");
            case LEFT_INCLUSIVE:
                return Pair.newPair(">=", "<");
            case RIGHT_INCLUSIVE:
                return Pair.newPair(">", "<=");
            default:
                throw new RuntimeException("error, unknown type for condition");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Cond cond = (Cond) o;

            if (type != cond.type)
                return false;
            if (leftExpr != null ? !leftExpr.equals(cond.leftExpr) : cond.leftExpr != null)
                return false;
            return rightExpr != null ? rightExpr.equals(cond.rightExpr) : cond.rightExpr == null;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (leftExpr != null ? leftExpr.hashCode() : 0);
            result = 31 * result + (rightExpr != null ? rightExpr.hashCode() : 0);
            return result;
        }

        static class RowACLCondSerializer extends JsonSerializer<Cond> {

            @Override
            public void serialize(Cond cond, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                Object[] c;
                if (cond.leftExpr.equals(cond.rightExpr)) {
                    c = new Object[2];
                    c[0] = cond.type.ordinal();
                    c[1] = cond.leftExpr;
                } else {
                    c = new Object[3];
                    c[0] = cond.type.ordinal();
                    c[1] = cond.leftExpr;
                    c[2] = cond.rightExpr;
                }
                gen.writeObject(c);
            }
        }

        static class RowACLCondDeserializer extends JsonDeserializer<Cond> {

            @Override
            public Cond deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                Object[] c = p.readValueAs(Object[].class);
                Cond cond = new Cond();
                cond.type = IntervalType.values()[(int) c[0]];
                if (c.length == 2) {
                    cond.leftExpr = cond.rightExpr = (String) c[1];
                } else {
                    cond.leftExpr = (String) c[1];
                    cond.rightExpr = (String) c[2];
                }
                return cond;
            }
        }
    }
}