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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISqlAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.mockup.AbstractQueryExecutor;

public abstract class AbstractSQLValidator {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSQLValidator.class);

    private static final int DEFAULT_THREAD_COUNT = 4;

    protected KylinConfig kylinConfig;
    private AbstractQueryExecutor queryExecutor;
    ISqlAdvisor sqlAdvisor;
    int threadCount;

    private AbstractSQLValidator(KylinConfig kylinConfig, AbstractQueryExecutor queryExecutor, int threadCount) {
        this.kylinConfig = kylinConfig;
        this.queryExecutor = queryExecutor;
        this.threadCount = threadCount;
    }

    AbstractSQLValidator(KylinConfig kylinConfig, AbstractQueryExecutor queryExecutor) {
        this(kylinConfig, queryExecutor, DEFAULT_THREAD_COUNT);
    }

    abstract AbstractQueryRunner createQueryRunner(String[] sqls) throws IOException;

    public Map<String, SQLValidateResult> batchValidate(String[] sqls) throws IOException {

        if (sqls == null || sqls.length == 0) {
            return Maps.newHashMap();
        }

        final AbstractQueryRunner queryRunner = createQueryRunner(sqls);
        try {
            queryRunner.execute(queryExecutor);
        } catch (Exception e) {
            logger.error("batch validate sql error" + Arrays.toString(sqls), e);
        }

        List<SQLResult> queryResults = queryRunner.getQueryResultList();
        List<Collection<OLAPContext>> olapContexts = queryRunner.getAllOLAPContexts();
        return doBatchValidate(sqls, queryResults, olapContexts);
    }

    private Map<String, SQLValidateResult> doBatchValidate(String[] sqls, List<SQLResult> queryResults,
            List<Collection<OLAPContext>> olapContexts) {

        Map<String, SQLValidateResult> validateStatsMap = Maps.newHashMap();
        for (int i = 0; i < sqls.length; i++) {
            SQLResult sqlResult = queryResults.get(i);
            validateStatsMap.put(sqls[i], doValidate(sqlResult, olapContexts.get(i)));
        }
        return validateStatsMap;
    }

    private SQLValidateResult doValidate(SQLResult sqlResult, Collection<OLAPContext> olapContexts) {

        List<SQLAdvice> sqlAdvices = sqlAdvisor.propose(sqlResult, olapContexts);
        if (CollectionUtils.isEmpty(sqlAdvices)) {
            return SQLValidateResult.successStats(sqlResult);
        }
        return SQLValidateResult.failedStats(sqlAdvices, sqlResult);
    }
}
