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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISqlAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.mockup.AbstractQueryExecutor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLValidator {

    @Getter
    protected final String project;
    protected KylinConfig kylinConfig;
    private final AbstractQueryExecutor queryExecutor;
    ISqlAdvisor sqlAdvisor;

    AbstractSQLValidator(String project, KylinConfig kylinConfig, AbstractQueryExecutor queryExecutor) {
        this.project = project;
        this.kylinConfig = kylinConfig;
        this.queryExecutor = queryExecutor;
    }

    abstract AbstractQueryRunner createQueryRunner(String[] sqls);

    public Map<String, SQLValidateResult> batchValidate(String[] sqls) {

        if (sqls == null || sqls.length == 0) {
            return Maps.newHashMap();
        }
        Map<String, SQLValidateResult> resultMap;
        try (AbstractQueryRunner queryRunner = createQueryRunner(sqls)) {
            try {
                queryRunner.execute(queryExecutor);
            } catch (Exception e) {
                log.error("batch validate sql error" + Arrays.toString(sqls), e);
            }
            List<SQLResult> queryResults = queryRunner.getQueryResultList();
            resultMap = doBatchValidate(sqls, queryResults);
        }
        return resultMap;
    }

    private Map<String, SQLValidateResult> doBatchValidate(String[] sqls, List<SQLResult> queryResults) {

        Map<String, SQLValidateResult> validateStatsMap = Maps.newHashMap();
        for (int i = 0; i < sqls.length; i++) {
            SQLResult sqlResult = queryResults.get(i);
            validateStatsMap.put(sqls[i], doValidate(sqlResult));
        }
        return validateStatsMap;
    }

    private SQLValidateResult doValidate(SQLResult sqlResult) {

        SQLAdvice sqlAdvice = sqlAdvisor.propose(sqlResult);
        if (Objects.isNull(sqlAdvice)) {
            return SQLValidateResult.successStats(sqlResult);
        }
        return SQLValidateResult.failedStats(Lists.newArrayList(sqlAdvice), sqlResult);
    }
}
