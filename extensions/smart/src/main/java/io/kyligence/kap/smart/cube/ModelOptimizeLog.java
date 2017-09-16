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

package io.kyligence.kap.smart.cube;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.smart.query.validator.SQLValidateResult;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ModelOptimizeLog extends RootPersistentEntity {
    @JsonProperty("model_name")
    private String modelName;

    @JsonProperty("sample_sqls")
    private List<String> sampleSqls = new ArrayList<>();

    @JsonProperty("sql_validate_results")
    private List<SQLValidateResult> sqlValidateResult = new ArrayList<>();

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getModelName() {
        return modelName;
    }

    public void setSampleSqls(List<String> sampleSqls) {
        this.sampleSqls = sampleSqls;
    }

    public List<String> getSampleSqls() {
        return sampleSqls;
    }

    public List<SQLValidateResult> getSqlValidateResult() {
        return sqlValidateResult;
    }

    public void setSqlValidateResult(List<SQLValidateResult> sqlValidateResult) {
        this.sqlValidateResult = sqlValidateResult;
    }

    public String getResourcePath() {
        return ModelOptimizeLogManager.MODEL_OPTIMIZE_LOG_STATISTICS_ROOT + "/" + modelName
                + MetadataConstants.FILE_SURFIX;
    }

    public List<String> getValidatedSqls() {
        List<String> validatedSqls = new ArrayList<>();

        if (sampleSqls == null || sqlValidateResult == null || sampleSqls.size() != sqlValidateResult.size()) {
            throw new IllegalStateException("Invalid sample_sqls and sql_validate_results info.");
        }

        for (int i = 0; i < sampleSqls.size(); i++) {
            if (sqlValidateResult.get(i).isCapable()) {
                validatedSqls.add(sampleSqls.get(i));
            }
        }

        return validatedSqls;
    }
}
