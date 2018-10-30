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

package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Setter
@Getter
public class SimplifiedMeasure implements Serializable {

    @JsonProperty("id")
    private int id;
    @JsonProperty("expression")
    private String expression;
    @JsonProperty("name")
    private String name;
    @JsonProperty("return_type")
    private String returnType;
    @JsonProperty("parameter_value")
    private List<ParameterResponse> parameterValue;
    @JsonProperty("converted_columns")
    private List<ColumnDesc> convertedColumns = new ArrayList<>();
    @JsonProperty("configuration")
    private Map<String, String> configuration;

    public static SimplifiedMeasure fromMeasure(NDataModel.Measure measure) {
        SimplifiedMeasure measureResponse = new SimplifiedMeasure();
        measureResponse.setId(measure.id);
        measureResponse.setName(measure.getName());
        measureResponse.setExpression(measure.getFunction().getExpression());
        measureResponse.setReturnType(measure.getFunction().getReturnType());
        List<ParameterResponse> parameters = new ArrayList<>();
        Set<ParameterDesc.PlainParameter> plainParameters = measure.getFunction().getParameter()
                .getPlainParameters();
        for (ParameterDesc.PlainParameter plainParameter : plainParameters) {
            ParameterResponse parameterResponse = new ParameterResponse();
            parameterResponse.setType(plainParameter.getType());
            parameterResponse.setValue(plainParameter.getValue());
            parameters.add(parameterResponse);
        }
        measureResponse.setConfiguration(measure.getFunction().getConfiguration());
        measureResponse.setParameterValue(parameters);
        return measureResponse;
    }

    public NDataModel.Measure toMeasure() {

        NDataModel.Measure measure = new NDataModel.Measure();
        measure.id = getId();
        measure.setName(getName());
        FunctionDesc functionDesc = new FunctionDesc();
        functionDesc.setReturnType(getReturnType());
        functionDesc.setExpression(getExpression());
        functionDesc.setConfiguration(configuration);
        List<ParameterResponse> parameterResponseList = getParameterValue();
        Collections.reverse(parameterResponseList);
        functionDesc.setParameter(enrichParameterDesc(parameterResponseList, null));
        measure.setFunction(functionDesc);
        return measure;
    }

    private ParameterDesc enrichParameterDesc(List<ParameterResponse> parameterResponseList,
                                              ParameterDesc nextParameterDesc) {
        if (CollectionUtils.isEmpty(parameterResponseList)) {
            return nextParameterDesc;
        }
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType(parameterResponseList.get(0).getType());
        parameterDesc.setValue(parameterResponseList.get(0).getValue());
        parameterDesc.setNextParameter(nextParameterDesc);
        if (parameterResponseList.size() >= 1) {
            return enrichParameterDesc(parameterResponseList.subList(1, parameterResponseList.size()), parameterDesc);
        } else {
            return parameterDesc;
        }

    }
}