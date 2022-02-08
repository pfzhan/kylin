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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OpenSuggestionResponse implements Serializable {

    @JsonProperty("models")
    private List<OpenModelRecResponse> models = Lists.newArrayList();

    @JsonProperty("error_sqls")
    private List<String> errorSqlList = Lists.newArrayList();

    public static List<OpenModelRecResponse> convert(List<SuggestionResponse.ModelRecResponse> response) {
        return response.stream().map(OpenModelRecResponse::convert).collect(Collectors.toList());
    }

    public static OpenSuggestionResponse from(SuggestionResponse innerResponse, List<String> sqls) {
        OpenSuggestionResponse result = new OpenSuggestionResponse();
        result.getModels().addAll(OpenSuggestionResponse.convert(innerResponse.getReusedModels()));
        result.getModels().addAll(OpenSuggestionResponse.convert(innerResponse.getNewModels()));
        result.fillErrorSqlList(sqls);
        return result;
    }

    private void fillErrorSqlList(List<String> inputSqlList) {
        Set<String> normalRecommendedSqlSet = Sets.newHashSet();
        for (OpenModelRecResponse modelResponse : getModels()) {
            modelResponse.getIndexes().forEach(layoutRecDetailResponse -> {
                List<String> sqlList = layoutRecDetailResponse.getSqlList();
                normalRecommendedSqlSet.addAll(sqlList);
            });
        }

        for (String sql : inputSqlList) {
            if (!normalRecommendedSqlSet.contains(sql)) {
                getErrorSqlList().add(sql);
            }
        }
    }
}
