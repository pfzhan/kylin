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
package io.kyligence.kap.rest.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.CubeSQLValidator;
import io.kyligence.kap.smart.query.validator.ModelSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

@Component("sqlValidateService")
public class SQLValidateService extends BasicService {
    public List<SQLValidateResult> validateModelSQL(List<String> sqlList, String modelName) {
        if (CollectionUtils.isEmpty(sqlList)) {
            return Collections.emptyList();
        }
        DataModelDesc modelDesc = getDataModelManager().getDataModelDesc(modelName);
        AbstractSQLValidator sqlValidator = new ModelSQLValidator(getConfig(), modelDesc);
        return transform(sqlList, sqlValidator.batchValidate(sqlList));
    }

    public List<SQLValidateResult> validateCubeSQL(List<String> sqlList, String cubeName) {
        if (CollectionUtils.isEmpty(sqlList)) {
            return Collections.emptyList();
        }
        CubeDesc cubeDesc = getCubeDescManager().getCubeDesc(cubeName);
        AbstractSQLValidator sqlValidator = new CubeSQLValidator(getConfig(), cubeDesc);
        return transform(sqlList, sqlValidator.batchValidate(sqlList));
    }

    private List<SQLValidateResult> transform(List<String> sqlList,
            Map<String, SQLValidateResult> sqlValidateResultMap) {
        List<SQLValidateResult> sqlValidateResult = Lists.newArrayList();
        for (String sql : sqlList) {
            sqlValidateResult.add(sqlValidateResultMap.get(sql));
        }
        return sqlValidateResult;
    }
}
