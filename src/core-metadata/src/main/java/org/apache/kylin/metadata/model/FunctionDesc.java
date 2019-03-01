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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Setter;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class FunctionDesc implements Serializable {

    public static FunctionDesc newInstance(String expression, ParameterDesc param, String returnType) {
        FunctionDesc r = new FunctionDesc();
        r.expression = (expression == null) ? null : expression.toUpperCase();
        r.parameter = param;
        r.returnType = returnType;
        r.returnDataType = DataType.getType(returnType);
        return r;
    }

    public static String proposeReturnType(String expression, String colDataType) {
        return proposeReturnType(expression, colDataType, Maps.newHashMap());
    }

    public static String proposeReturnType(String expression, String colDataType, Map<String, String> override) {
        String returnType = override.getOrDefault(expression,
                EXPRESSION_DEFAULT_TYPE_MAP.getOrDefault(expression, colDataType));
        switch (expression) {
        case FunctionDesc.FUNC_SUM:
            if (colDataType != null) {
                DataType type = DataType.getType(returnType);
                if (type.isIntegerFamily()) {
                    returnType = TYPE_BIGINT;
                } else if (type.isDecimal()) {
                    returnType = String.format("decimal(19,%d)", type.getScale());
                }
            } else {
                returnType = "decimal(19,4)";
            }
            break;
        default:
            break;
        }
        return returnType;
    }

    public static final String TYPE_BIGINT = "bigint";

    public static final String FUNC_SUM = "SUM";
    public static final String FUNC_MIN = "MIN";
    public static final String FUNC_MAX = "MAX";
    public static final String FUNC_COUNT = "COUNT";
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final String FUNC_COUNT_DISTINCT_HLLC10 = "hllc(10)";
    public static final String FUNC_COUNT_DISTINCT_BIT_MAP = "bitmap";
    public static final String FUNC_PERCENTILE = "PERCENTILE_APPROX";
    public static final String FUNC_GROUPING = "GROUPING";
    public static final String FUNC_TOP_N = "TOP_N";
    public static final ImmutableSet<String> DIMENSION_AS_MEASURES = ImmutableSet.<String> builder()
            .add(FUNC_MAX, FUNC_MIN, FUNC_COUNT_DISTINCT).build();
    public static final ImmutableSet<String> BUILT_IN_AGGREGATIONS = ImmutableSet.<String> builder()
            .add(FUNC_MAX, FUNC_MIN, FUNC_COUNT_DISTINCT).add(FUNC_COUNT, FUNC_SUM, FUNC_PERCENTILE).build();

    private static final Map<String, String> EXPRESSION_DEFAULT_TYPE_MAP = Maps.newHashMap();

    static {
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_TOP_N, "topn(100, 4)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT_DISTINCT, FUNC_COUNT_DISTINCT_BIT_MAP);
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_PERCENTILE, "percentile(100)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT, TYPE_BIGINT);
    }

    public static final String PARAMETER_TYPE_CONSTANT = "constant";
    public static final String PARAMETER_TYPE_COLUMN = "column";
    public static final String PARAMETER_TYPE_MATH_EXPRESSION = "math_expression";

    @JsonProperty("expression")
    private String expression;
    @JsonProperty("parameter")
    private ParameterDesc parameter;
    @JsonProperty("returntype")
    private String returnType;

    @Setter
    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = Maps.newLinkedHashMap();

    private DataType returnDataType;
    private MeasureType<?> measureType;
    private boolean isDimensionAsMetric = false;

    public void init(NDataModel model) {
        expression = expression.toUpperCase();
        if (expression.equals(PercentileMeasureType.FUNC_PERCENTILE)) {
            expression = PercentileMeasureType.FUNC_PERCENTILE_APPROX; // for backward compatibility
        }

        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            if (p.isColumnType()) {
                TblColRef colRef = model.findColumn(p.getValue());
                returnDataType = DataType.getType(proposeReturnType(expression, colRef.getDatatype()));
                p.setValue(colRef.getIdentity());
                p.setColRef(colRef);
            }
        }
        if (returnDataType == null) {
            returnDataType = DataType.getType(TYPE_BIGINT);
        }
        if (!StringUtils.isEmpty(returnType)) {
            returnDataType = DataType.getType(returnType);
        }
        returnType = returnDataType.toString();
    }

    private void reInitMeasureType() {
        if (isDimensionAsMetric && isCountDistinct()) {
            // create DimCountDis
            measureType = MeasureTypeFactory.createNoRewriteFieldsMeasureType(getExpression(), getReturnDataType());
            returnDataType = DataType.getType("dim_dc");
        } else {
            measureType = MeasureTypeFactory.create(getExpression(), getReturnDataType());
        }
    }

    public MeasureType getMeasureType() {
        //like max(cal_dt)
        if (isDimensionAsMetric && !isCountDistinct()) {
            return null;
        }

        if (measureType == null) {
            reInitMeasureType();
        }

        return measureType;
    }

    public boolean needRewrite() {
        if (getMeasureType() == null)
            return false;

        return getMeasureType().needRewrite();
    }

    public boolean needRewriteField() {
        if (!needRewrite())
            return false;

        return getMeasureType().needRewriteField();
    }

    public String getRewriteFieldName() {
        if (isCountConstant()) {
            return "_KY_" + "COUNT__"; // ignores parameter, count(*) and count(1) are the same
        } else if (isCountDistinct()) {
            return "_KY_" + getFullExpressionInAlphabetOrder().replaceAll("[(),. ]", "_");
        } else {
            return "_KY_" + getFullExpression().replaceAll("[(),. ]", "_");
        }
    }

    public DataType getRewriteFieldType() {
        if (getMeasureType() instanceof BasicMeasureType) {
            if (isMax() || isMin()) {
                return parameter.getColRefs().get(0).getType();
            } else if (isSum()) {
                if (parameter.isConstant())
                    return returnDataType;

                return parameter.getColRefs().get(0).getType();
            } else if (isCount()) {
                return DataType.getType(TYPE_BIGINT);
            } else {
                throw new IllegalArgumentException("unknown measure type " + getMeasureType());
            }
        } else {
            return DataType.ANY;
        }
    }

    public ColumnDesc newFakeRewriteColumn(String rewriteFiledName, TableDesc sourceTable) {
        ColumnDesc fakeCol = new ColumnDesc();
        fakeCol.setName(rewriteFiledName);
        fakeCol.setDatatype(getRewriteFieldType().toString());
        if (isCountConstant())
            fakeCol.setNullable(false);
        fakeCol.init(sourceTable);
        return fakeCol;
    }

    public ColumnDesc newFakeRewriteColumn(TableDesc sourceTable) {
        return newFakeRewriteColumn(getRewriteFieldName(), sourceTable);
    }

    public boolean isMin() {
        return FUNC_MIN.equalsIgnoreCase(expression);
    }

    public boolean isMax() {
        return FUNC_MAX.equalsIgnoreCase(expression);
    }

    public boolean isSum() {
        return FUNC_SUM.equalsIgnoreCase(expression);
    }

    public boolean isCount() {
        return FUNC_COUNT.equalsIgnoreCase(expression);
    }

    public boolean isCountOnColumn() {
        return FUNC_COUNT.equalsIgnoreCase(expression) && parameter != null && parameter.isColumnType();
    }

    public boolean isAggregateOnConstant() {
        return !this.isCount() && parameter != null && parameter.isConstantParameterDesc();
    }

    public boolean isCountConstant() {//count(*) and count(1)
        return FUNC_COUNT.equalsIgnoreCase(expression) && (parameter == null || parameter.isConstant());
    }

    public boolean isCountDistinct() {
        return FUNC_COUNT_DISTINCT.equalsIgnoreCase(expression);
    }

    public boolean isGrouping() {
        return FUNC_GROUPING.equalsIgnoreCase(expression);
    }

    /**
     * Get Full Expression such as sum(amount), count(1), count(*)...
     */
    public String getFullExpression() {
        StringBuilder sb = new StringBuilder(expression);
        sb.append("(");
        ParameterDesc desc = parameter;
        while (desc != null) {
            sb.append(desc.getValue());
            desc = desc.getNextParameter();
            if (desc != null)
                sb.append(",");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Parameters' name appears in alphabet order.
     * This method is used for funcs whose parameters appear in arbitrary order
     */
    public String getFullExpressionInAlphabetOrder() {
        StringBuilder sb = new StringBuilder(expression);
        sb.append("(");
        ParameterDesc localParam = parameter;
        List<String> flatParams = Lists.newArrayList();
        while (localParam != null) {
            flatParams.add(localParam.getValue());
            localParam = localParam.getNextParameter();
        }
        Collections.sort(flatParams);
        sb.append(Joiner.on(",").join(flatParams));
        sb.append(")");
        return sb.toString();
    }

    public boolean isDimensionAsMetric() {
        return isDimensionAsMetric;
    }

    public void setDimensionAsMetric(boolean isDimensionAsMetric) {
        this.isDimensionAsMetric = isDimensionAsMetric;
        if (measureType != null) {
            reInitMeasureType();
        }
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public ParameterDesc getParameter() {
        return parameter;
    }

    public void setParameter(ParameterDesc parameter) {
        this.parameter = parameter;
    }

    public int getParameterCount() {
        int count = 0;
        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            count++;
        }
        return count;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public DataType getReturnDataType() {
        return returnDataType;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((isCount() || parameter == null) ? 0 : parameter.hashCode());
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FunctionDesc other = (FunctionDesc) obj;
        if (expression == null) {
            if (other.expression != null)
                return false;
        } else if (!expression.equals(other.expression))
            return false;
        if (isCountDistinct()) {
            // for count distinct func, param's order doesn't matter
            if (parameter == null) {
                if (other.parameter != null)
                    return false;
            } else {
                return parameter.equalInArbitraryOrder(other.parameter);
            }
        } else if (isCountConstant() && ((FunctionDesc) obj).isCountConstant()) { //count(*) and count(1) are equals
            return true;
        } else {
            if (parameter == null) {
                if (other.parameter != null)
                    return false;
            } else {
                if (!parameter.equals(other.parameter))
                    return false;
            }
        }
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
        return true;
    }

    @Override
    public String toString() {
        return "FunctionDesc [expression=" + expression + ", parameter=" + parameter + ", returnType=" + returnType
                + "]";
    }
}
