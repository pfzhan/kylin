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

package io.kyligence.kap.metadata.query;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryHistorySql {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("normalized_sql")
    private String normalizedSql;

    @JsonProperty("params")
    private List<QueryHistorySqlParam> params;

    @JsonIgnore
    public String getSqlWithParameterBindingComment() {
        if (CollectionUtils.isEmpty(params)) {
            return sql;
        }
        StringBuilder sb = new StringBuilder(sql);
        sb.append(LINE_SEPARATOR);
        sb.append(LINE_SEPARATOR);
        sb.append("-- [PARAMETER BINDING]");
        sb.append(LINE_SEPARATOR);
        for (QueryHistorySqlParam p : params) {
            sb.append(String.format("-- Binding parameter [%s] as [%s] - [%s]", p.getPos(), p.getDataType(), p.getValue()));
            sb.append(LINE_SEPARATOR);
        }
        sb.append("-- [PARAMETER BINDING END]");

        return sb.toString();
    }
}
