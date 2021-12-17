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
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class NCubeDescResponse implements Serializable {
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("dimensions")
    private List<Dimension3X> dimensions;
    @JsonProperty("measures")
    private List<Measure3X> measures;
    @JsonProperty("aggregation_groups")
    private List<AggGroupResponse> aggregationGroups;

    @Data
    public static class Measure3X implements Serializable {
        @JsonProperty("name")
        private String name;
        @JsonProperty("description")
        private String description;
        @JsonProperty("function")
        private FunctionDesc3X functionDesc3X;

        public Measure3X() {
        }

        public Measure3X(NDataModel.Measure measure) {
            this.setName(measure.getName());
            this.setDescription("");
            this.setFunctionDesc3X(new FunctionDesc3X(measure.getFunction()));
        }
    }

    @Data
    public static class Dimension3X implements Serializable {
        @JsonProperty("name")
        private String name;
        @JsonProperty("table")
        private String table;
        @JsonProperty("column")
        private String column;
        @JsonProperty("derived")
        private List<String> derived;

        public Dimension3X() {
        }

        public Dimension3X(NDataModel.NamedColumn namedColumn, boolean isDerived) {
            this.name = namedColumn.getName();
            this.table = namedColumn.getAliasDotColumn().split("\\.")[0].toUpperCase(Locale.ROOT).trim();
            String columnName = namedColumn.getAliasDotColumn().split("\\.")[1].toUpperCase(Locale.ROOT).trim();
            if (!isDerived) {
                this.column = columnName;
                this.derived = null;
            } else {
                this.column = null;
                this.derived = Collections.singletonList(columnName);
            }

        }
    }

    @Data
    public static class FunctionDesc3X implements Serializable {
        @JsonProperty("expression")
        private String expression;
        @JsonProperty("parameter")
        private ParameterDesc3X parameter;
        @JsonProperty("returntype")
        private String returnType;

        public FunctionDesc3X() {
        }

        public FunctionDesc3X(FunctionDesc functionDesc) {
            this.setParameter(ParameterDesc3X.convert(functionDesc.getParameters()));
            this.setExpression(functionDesc.getExpression());
            this.setReturnType(functionDesc.getReturnType());
        }
    }

    @Data
    public static class ParameterDesc3X implements Serializable {
        @Getter
        @Setter
        @JsonProperty("type")
        private String type;
        @Getter
        @Setter
        @JsonProperty("value")
        private String value;

        @JsonProperty("next_parameter")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private ParameterDesc3X nextParameter;

        public static ParameterDesc3X convert(ParameterDesc parameterDesc) {
            ParameterDesc3X parameterDesc3X = new ParameterDesc3X();
            parameterDesc3X.setType(parameterDesc.getType());
            parameterDesc3X.setValue(parameterDesc.getValue());
            return parameterDesc3X;
        }

        public static ParameterDesc3X convert(List<ParameterDesc> parameterDescs) {
            if (CollectionUtils.isEmpty(parameterDescs)) {
                return new ParameterDesc3X();
            }

            ParameterDesc3X head = null;
            ParameterDesc3X tail = null;
            for (ParameterDesc parameterDesc : parameterDescs) {
                if (null == head) {
                    head = convert(parameterDesc);
                    tail = head;
                    continue;
                }

                tail.nextParameter = convert(parameterDesc);
            }

            return head;
        }
    }

}
