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
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import lombok.Data;

@Data
public class ComputedColumnDescResponse {

    @JsonProperty("table_identity")
    private String tableIdentity;

    @JsonProperty("table_alias")
    private String tableAlias;
    @JsonProperty("column_name")
    private String columnName; // the new col name
    @JsonProperty
    private String expression;
    @JsonProperty("inner_expression")
    private String innerExpression; // QueryUtil massaged expression
    @JsonProperty("data_type")
    private String dataType;
    @JsonProperty
    private String comment;

    public static ComputedColumnDescResponse convert(ComputedColumnDesc computedColumnDesc) {
        ComputedColumnDescResponse response = new ComputedColumnDescResponse();
        response.setTableIdentity(computedColumnDesc.getTableIdentity());
        response.setTableAlias(computedColumnDesc.getTableAlias());
        response.setColumnName(computedColumnDesc.getColumnName());
        response.setExpression(computedColumnDesc.getExpression());
        response.setInnerExpression(computedColumnDesc.getInnerExpression());
        response.setDataType(computedColumnDesc.getDatatype());
        response.setComment(computedColumnDesc.getComment());

        return response;
    }

}
