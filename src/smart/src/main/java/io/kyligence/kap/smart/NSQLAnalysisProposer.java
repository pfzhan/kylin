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

package io.kyligence.kap.smart;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.realization.NoRealizationFoundException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.NQueryRunnerFactory;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISqlAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.advisor.SqlSyntaxAdvisor;

class NSQLAnalysisProposer extends NAbstractProposer {

    private static final int DEFAULT_THREAD_NUM = 1;

    private final String[] sqls;

    NSQLAnalysisProposer(NSmartContext smartContext) {
        super(smartContext);

        this.sqls = smartContext.getSqls();
    }

    @Override
    void propose() {
        initAccelerationInfo(sqls);
        List<NDataModel> models = kylinConfig.isSemiAutoMode() ? Lists.newArrayList() : getOriginModels();
        try (AbstractQueryRunner extractor = NQueryRunnerFactory.createForModelSuggestion(kylinConfig, project, sqls,
                models, DEFAULT_THREAD_NUM)) {
            extractor.execute();
            logFailedQuery(extractor);

            final List<NModelContext> modelContexts = new GreedyModelTreesBuilder(kylinConfig, project, smartContext)
                    .build(Arrays.asList(sqls), extractor.getAllOLAPContexts(), null).stream()
                    .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()).map(smartContext::createModelContext)
                    .collect(Collectors.toList());

            smartContext.setModelContexts(modelContexts);
        } catch (Exception e) {
            logger.error("Failed to get query stats. ", e);
        }
    }

    /**
     * Init acceleration info
     */
    private void initAccelerationInfo(String[] sqls) {
        Arrays.stream(sqls).forEach(sql -> {
            AccelerateInfo accelerateInfo = new AccelerateInfo();
            if (!smartContext.getAccelerateInfoMap().containsKey(sql)) {
                smartContext.getAccelerateInfoMap().put(sql, accelerateInfo);
            }
        });
    }

    private void logFailedQuery(AbstractQueryRunner extractor) {
        final Map<Integer, SQLResult> queryResultMap = extractor.getQueryResults();
        ISqlAdvisor sqlAdvisor = new SqlSyntaxAdvisor();

        queryResultMap.forEach((index, sqlResult) -> {
            if (sqlResult.getStatus() == SQLResult.Status.FAILED) {
                AccelerateInfo accelerateInfo = smartContext.getAccelerateInfoMap().get(sqls[index]);
                Preconditions.checkNotNull(accelerateInfo);
                Throwable throwable = sqlResult.getException();
                if (!(throwable instanceof NoRealizationFoundException
                        || throwable.getCause() instanceof NoRealizationFoundException)) {
                    if (throwable.getMessage().contains("not found")) {
                        SQLAdvice sqlAdvices = sqlAdvisor.propose(sqlResult);
                        accelerateInfo.setPendingMsg(sqlAdvices.getIncapableReason());
                    } else {
                        accelerateInfo.setFailedCause(throwable);
                    }
                }
            }
        });
    }
}
