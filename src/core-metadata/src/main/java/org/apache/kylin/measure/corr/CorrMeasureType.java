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

package org.apache.kylin.measure.corr;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_SUM;
import static org.apache.kylin.metadata.model.FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION;

public class CorrMeasureType extends MeasureType {

    public static final String FUNC_CORR = "CORR";

    public static final String DATATYPE_CORR = "corr";

    public static class Factory extends MeasureTypeFactory {

        @Override
        public MeasureType createMeasureType(String funcName, DataType dataType) {
            return new CorrMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_CORR;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_CORR;
        }

        @Override
        public Class<? extends DataTypeSerializer> getAggrDataTypeSerializer() {
            return null;
        }
    }


    @Override
    public MeasureIngester newIngester() {
        return null;
    }

    @Override
    public MeasureAggregator newAggregator() {
        return null;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    private static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.of(
            FUNC_CORR, CorrAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public boolean expandable() {
        return true;
    }

    @Override
    public List<FunctionDesc> convertToInternalFunctionDesc(FunctionDesc functionDesc) {
        List<ParameterDesc> parameterDescList = Lists.newArrayList();
        for (ParameterDesc parameter : functionDesc.getParameters()) {
            parameter.getColRef();
            parameterDescList.add(parameter);
        }

        List<FunctionDesc> functionDescs = Lists.newArrayList();

        //convert CORR to SUM（X*X), SUM（X*Y), SUM（Y*Y)
        List<ParameterDesc> descs = Lists.newArrayList();
        for (int i = 0; i < parameterDescList.size(); i++) {
            for (int j = i; j < parameterDescList.size(); j++) {
                ParameterDesc newParam = new ParameterDesc();
                newParam.setType(PARAMETER_TYPE_MATH_EXPRESSION);
                newParam.setValue(
                        String.format(Locale.ROOT, "%s * %s", parameterDescList.get(i).getValue(), parameterDescList.get(j).getValue()));
                descs.add(newParam);
            }
        }
        parameterDescList.addAll(descs);

        for (ParameterDesc param : parameterDescList) {
            FunctionDesc function = FunctionDesc.newInstance(FUNC_SUM, Lists.newArrayList(param), null);
            functionDescs.add(function);
        }
        return functionDescs;
    }
}
