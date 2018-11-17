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

import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.query.SQLResult;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.NQueryRunnerFactory;

import java.util.Map;

class NSQLAnalysisProposer extends NAbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(NSQLAnalysisProposer.class);
    private NSQLSimpleClusterer clusterer = new NSQLSimpleClusterer();

    NSQLAnalysisProposer(NSmartContext context) {
        super(context);
    }

    @Override
    void propose() {
        try (AbstractQueryRunner extractor = NQueryRunnerFactory.createForModelSuggestion(context.getKylinConfig(),
                context.getProject(), context.getSqls(), 1)) {
            extractor.execute();
            logFailedQuery(extractor);
            context.setOlapContexts(extractor.getOlapContexts());
            context.setModelContexts(clusterer.cluster(context));
        } catch (Exception e) {
            logger.error("Failed to get query stats. ", e);
        }
    }

    private void logFailedQuery(AbstractQueryRunner extractor) {
        final Map<Integer, SQLResult> queryResultMap = extractor.getQueryResults();

        queryResultMap.forEach((index, sqlResult) -> {
            if (sqlResult.getStatus() == SQLResult.Status.FAILED) {
                AccelerateInfo accelerateInfo = new AccelerateInfo();
                Throwable throwable = sqlResult.getException();
                if (!(throwable instanceof NoRealizationFoundException
                        || throwable.getCause() instanceof NoRealizationFoundException)) {
                    accelerateInfo.setBlockingCause(sqlResult.getException());
                }
                if (!context.getAccelerateInfoMap().containsKey(context.getSqls()[index])) {
                    context.getAccelerateInfoMap().put(context.getSqls()[index], accelerateInfo);
                }
            }
        });
    }
}
