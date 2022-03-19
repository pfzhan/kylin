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

package io.kyligence.kap.smart.query.validator;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISqlAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLValidator {

    @Getter
    protected final String project;
    protected KylinConfig kylinConfig;
    ISqlAdvisor sqlAdvisor;

    AbstractSQLValidator(String project, KylinConfig kylinConfig) {
        this.project = project;
        this.kylinConfig = kylinConfig;
    }

    abstract AbstractQueryRunner createQueryRunner(String[] sqls);

    public Map<String, SQLValidateResult> batchValidate(String[] sqls) {
        if (ArrayUtils.isEmpty(sqls)) {
            return Maps.newHashMap();
        }
        Map<String, SQLValidateResult> resultMap;
        try (AbstractQueryRunner queryRunner = createQueryRunner(sqls)) {
            try {
                queryRunner.execute();
            } catch (Exception e) {
                log.error("batch validate sql error" + Arrays.toString(sqls), e);
            }
            resultMap = advice(queryRunner.getQueryResults());
        }
        return resultMap;
    }

    private Map<String, SQLValidateResult> advice(Map<String, SQLResult> queryResultMap) {

        Map<String, SQLValidateResult> validateStatsMap = Maps.newHashMap();
        queryResultMap.forEach((sql, sqlResult) -> {
            SQLAdvice advice = sqlAdvisor.propose(sqlResult);
            SQLValidateResult result = Objects.isNull(advice) //
                    ? SQLValidateResult.successStats(sqlResult)
                    : SQLValidateResult.failedStats(Lists.newArrayList(advice), sqlResult);
            validateStatsMap.put(sql, result);
        });
        return validateStatsMap;
    }
}
