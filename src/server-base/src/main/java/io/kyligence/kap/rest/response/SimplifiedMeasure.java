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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import lombok.val;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@EqualsAndHashCode
public class SimplifiedMeasure implements Serializable {

    @EqualsAndHashCode.Exclude
    @JsonProperty("id")
    private int id;
    @JsonProperty("expression")
    private String expression;
    @EqualsAndHashCode.Exclude
    @JsonProperty("name")
    private String name;
    // returnType is concerned in equal comparasion for return type changes in measure
    // see io.kyligence.kap.rest.service.ModelServiceSemanticUpdateTest.testUpdateMeasure_ChangeReturnType
    @JsonProperty("return_type")
    private String returnType;
    @JsonProperty("parameter_value")
    private List<ParameterResponse> parameterValue;
    @EqualsAndHashCode.Exclude
    @JsonProperty("converted_columns")
    private List<ColumnDesc> convertedColumns = new ArrayList<>();
    @EqualsAndHashCode.Exclude
    @JsonProperty("column")
    private String column;
    @EqualsAndHashCode.Exclude
    @JsonProperty("comment")
    private String comment;
    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = Maps.newHashMap();

    public static SimplifiedMeasure fromMeasure(NDataModel.Measure measure) {
        SimplifiedMeasure measureResponse = new SimplifiedMeasure();
        measureResponse.setId(measure.getId());
        measureResponse.setName(measure.getName());
        measureResponse.setExpression(measure.getFunction().getExpression());
        measureResponse.setReturnType(measure.getFunction().getReturnType());
        List<ParameterResponse> parameters = measure.getFunction().getParameters().stream()
                .map(parameterDesc -> new ParameterResponse(parameterDesc.getType(), parameterDesc.getValue()))
                .collect(Collectors.toList());
        measureResponse.setConfiguration(measure.getFunction().getConfiguration());
        measureResponse.setParameterValue(parameters);
        measureResponse.setColumn(measure.getColumn());
        measureResponse.setComment(measure.getComment());
        return measureResponse;
    }

    public NDataModel.Measure toMeasure() {
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setId(getId());
        measure.setName(getName());
        measure.setColumn(getColumn());
        measure.setComment(getComment());
        FunctionDesc functionDesc = new FunctionDesc();
        functionDesc.setReturnType(getReturnType());
        functionDesc.setExpression(getExpression());
        functionDesc.setConfiguration(configuration);
        List<ParameterResponse> parameterResponseList = getParameterValue();

        // transform parameter response to parameter desc
        List<ParameterDesc> parameterDescs = parameterResponseList.stream().map(parameterResponse -> {
            ParameterDesc parameterDesc = new ParameterDesc();
            parameterDesc.setType(parameterResponse.getType());
            parameterDesc.setValue(parameterResponse.getValue());
            return parameterDesc;
        }).collect(Collectors.toList());
        functionDesc.setParameters(parameterDescs);
        measure.setFunction(functionDesc);
        return measure;
    }

    public void changeTableAlias(String oldAlias, String newAlias) {
        for (val parameter : parameterValue) {
            String table = parameter.getValue().split("\\.")[0];
            if (oldAlias.equalsIgnoreCase(table)) {
                String column = parameter.getValue().split("\\.")[1];
                parameter.setValue(newAlias + "." + column);
            }
        }
    }
}