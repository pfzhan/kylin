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

package io.kyligence.kap.query.runtime;


import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.query.engine.exec.ExecuteResult;
import io.kyligence.kap.query.engine.exec.sparder.QueryEngine;
import io.kyligence.kap.query.mask.QueryResultMasks;
import io.kyligence.kap.query.runtime.plan.ResultPlan;

public class SparkEngine implements QueryEngine {
    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);

    private Dataset<Row> toSparkPlan(DataContext dataContext, RelNode relNode) {
        QueryContext.currentTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        log.info("Begin planning spark plan.");
        long start = System.currentTimeMillis();
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext);
        try {
            calciteToSparkPlaner.go(relNode);
        } finally {
            calciteToSparkPlaner.cleanCache();
        }
        long takeTime = System.currentTimeMillis() - start;
        QueryContext.current().record("to_spark_plan");
        log.info("Plan take {} ms", takeTime);
        return calciteToSparkPlaner.getResult();
    }

    @Override
    public List<List<String>> compute(DataContext dataContext, RelNode relNode) {
        return ImmutableList.copyOf(computeToIterable(dataContext, relNode).getRows());
    }

    @Override
    public ExecuteResult computeToIterable(DataContext dataContext, RelNode relNode) {
        Dataset<Row> sparkPlan = QueryResultMasks.maskResult(toSparkPlan(dataContext, relNode));
        log.info("SPARK LOGICAL PLAN {}", sparkPlan.queryExecution().logical());
        if (KapConfig.getInstanceFromEnv().isOnlyPlanInSparkEngine()) {
            return ResultPlan.completeResultForMdx(sparkPlan, relNode.getRowType());
        } else {
            return ResultPlan.getResult(sparkPlan, relNode.getRowType());
        }
    }
}
