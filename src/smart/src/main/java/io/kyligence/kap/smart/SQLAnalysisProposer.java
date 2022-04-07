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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerBuilder;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.advisor.SqlSyntaxAdvisor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLAnalysisProposer extends AbstractProposer {

    private final String[] sqls;

    public SQLAnalysisProposer(AbstractContext proposeContext) {
        super(proposeContext);
        this.sqls = Objects.requireNonNull(proposeContext.getSqlArray());
    }

    @Override
    public void execute() {
        initAccelerationInfo(sqls);
        List<NDataModel> models = proposeContext.getOriginModels();
        try (AbstractQueryRunner extractor = new QueryRunnerBuilder(project,
                getProposeContext().getSmartConfig().getKylinConfig(), sqls).of(models).build()) {
            extractor.execute();
            logFailedQuery(extractor);

            val modelContexts = new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext) //
                    .build(extractor.filterNonModelViewOlapContexts(), null) //
                    .stream() //
                    .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()) //
                    .map(proposeContext::createModelContext) //
                    .collect(Collectors.toList());
            proposeContext.getModelViewOLAPContextMap().putAll(extractor.filterModelViewOLAPContexts());
            proposeContext.setModelContexts(modelContexts);
        } catch (Exception e) {
            log.error("Failed to get query stats. ", e);
        }
    }

    /**
     * Init acceleration info
     */
    private void initAccelerationInfo(String[] sqls) {
        Arrays.stream(sqls).forEach(sql -> {
            AccelerateInfo accelerateInfo = new AccelerateInfo();
            if (!proposeContext.getAccelerateInfoMap().containsKey(sql)) {
                proposeContext.getAccelerateInfoMap().put(sql, accelerateInfo);
            }
        });
    }

    private void logFailedQuery(AbstractQueryRunner extractor) {
        final Map<String, SQLResult> queryResultMap = extractor.getQueryResults();
        SqlSyntaxAdvisor sqlAdvisor = new SqlSyntaxAdvisor();

        queryResultMap.forEach((sql, sqlResult) -> {
            if (sqlResult.getStatus() != SQLResult.Status.FAILED) {
                return;
            }
            AccelerateInfo accelerateInfo = proposeContext.getAccelerateInfoMap().get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            Throwable throwable = sqlResult.getException();
            if (!(throwable instanceof NoRealizationFoundException
                    || throwable.getCause() instanceof NoRealizationFoundException)) {
                if (throwable.getMessage().contains("not found")) {
                    SQLAdvice sqlAdvices = sqlAdvisor.proposeWithMessage(sqlResult);
                    accelerateInfo.setPendingMsg(sqlAdvices.getIncapableReason());
                } else {
                    accelerateInfo.setFailedCause(throwable);
                }
            }
        });
    }

    @Override
    public String getIdentifierName() {
        return "SQLAnalysisProposer";
    }
}
