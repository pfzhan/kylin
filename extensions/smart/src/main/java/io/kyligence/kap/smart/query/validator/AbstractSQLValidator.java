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
import io.kyligence.kap.smart.query.advisor.ISQLAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;

public abstract class AbstractSQLValidator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSQLValidator.class);

    protected KylinConfig kylinConfig;
    protected ISQLAdvisor sqlAdvisor;
    protected int threadCount;

    public AbstractSQLValidator(KylinConfig kylinConfig, int threadCount) {
        this.kylinConfig = kylinConfig;
        this.threadCount = threadCount;
    }

    public AbstractSQLValidator(KylinConfig kylinConfig) {
        this(kylinConfig, 4);
    }

    protected abstract ISQLAdvisor getSQLAdvisor();

    protected abstract AbstractQueryRunner createQueryRunner(String[] sqls);

    protected SQLValidateResult doValidate(SQLResult sqlResult, Collection<OLAPContext> olapContexts) {
        List<SQLAdvice> sqlAdvices = getSQLAdvisor().provideAdvice(sqlResult, olapContexts);
        if (CollectionUtils.isEmpty(sqlAdvices)) {
            return SQLValidateResult.successStats();
        } else {
            return SQLValidateResult.failedStats(sqlAdvices);
        }
    }

    public Map<String, SQLValidateResult> batchValidate(List<String> sqlList) {
        if (CollectionUtils.isEmpty(sqlList)) {
            return Maps.newHashMap();
        }
        String[] sqlArray = sqlList.toArray(new String[sqlList.size()]);
        AbstractQueryRunner queryRunner = createQueryRunner(sqlArray);
        try {

            queryRunner.execute();
        } catch (Exception e) {
            logger.error("batch validate sql error" + Arrays.toString(sqlArray), e);
        }

        List<SQLResult> queryResults = queryRunner.getQueryResults();
        List<Collection<OLAPContext>> olapContexts = queryRunner.getAllOLAPContexts();
        return doBatchValidate(sqlList, queryResults, olapContexts);
    }

    protected Map<String, SQLValidateResult> doBatchValidate(List<String> sqlList, List<SQLResult> queryResults,
            List<Collection<OLAPContext>> olapContexts) {
        Map<String, SQLValidateResult> validateStatsMap = Maps.newHashMap();
        for (int i = 0; i < sqlList.size(); i++) {
            SQLResult sqlResult = queryResults.get(i);
            validateStatsMap.put(sqlList.get(i), doValidate(sqlResult, olapContexts.get(i)));
        }
        return validateStatsMap;
    }
}
