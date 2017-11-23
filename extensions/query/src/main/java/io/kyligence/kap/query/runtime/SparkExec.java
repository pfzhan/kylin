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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.common.SparderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.query.relnode.KapContext;
import io.kyligence.kap.query.relnode.KapRel;

public class SparkExec implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(SparkExec.class);

    public static Enumerable<Object[]> collectToEnumerable(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }

        KapRel kapRel = KapContext.getKapRel();
        RelDataType rowType = KapContext.getRowType();

        Dataset<Row> df = createDataFrame(dataContext, kapRel);
        return SparderRuntime$.MODULE$.collectEnumerable(df, rowType);
    }

    public static Enumerable<Object> collectToScalarEnumerable(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }

        KapRel kapRel = KapContext.getKapRel();
        RelDataType rowType = KapContext.getRowType();

        Dataset<Row> df = createDataFrame(dataContext, kapRel);
        return SparderRuntime$.MODULE$.collectScalarEnumerable(df, rowType);
    }

    public static Enumerable<Object[]> asyncResult(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }
        String path = KapConfig.getInstanceFromEnv().getAsyncResultBaseDir() + "/" + QueryContext.current().getQueryId();
        KapRel kapRel = KapContext.getKapRel();
        RelDataType rowType = KapContext.getRowType();
        Dataset<Row> df = createDataFrame(dataContext, kapRel);
        return SparderRuntime$.MODULE$.asyncResult(df, rowType, SparderContext.getSeparator(), path);
    }

    private static Dataset<Row> createDataFrame(DataContext dataContext, KapRel kapRel) {
        long start = System.currentTimeMillis();
        Dataset<Row> df = new KapRel.SparderImplementor(dataContext).visitChild(kapRel);
        logger.info("sparder plan: \n" + df.queryExecution().analyzed());
        LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();
        logger.info("sparder optimized plan: \n" + optimizedPlan);
        logger.info("Time cost on sparder planning: " + (System.currentTimeMillis() - start));

        return df;
    }
}
