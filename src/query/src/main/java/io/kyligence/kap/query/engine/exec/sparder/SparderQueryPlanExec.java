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

package io.kyligence.kap.query.engine.exec.sparder;

import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import io.kyligence.kap.query.engine.exec.QueryPlanExec;
import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.relnode.KapContext;
import io.kyligence.kap.query.relnode.KapRel;
import io.kyligence.kap.query.util.QueryContextCutter;

/**
 * implement and execute a physical plan with Sparder
 */
public class SparderQueryPlanExec implements QueryPlanExec {

    public List<List<String>> execute(RelNode rel, DataContext dataContext) {
        selectRealizationAndRewrite(rel);
        return doExecute(rel, dataContext);
    }

    /**
     * submit rel and dataContext to query engine
     * @param rel
     * @param dataContext
     * @return
     */
    private List<List<String>> doExecute(RelNode rel, DataContext dataContext) {
        // create SparkEngine with reflection since kap-sparder is dependent on kap-query
        // so that kap-query cannot reference classes from kap-sparder normally
        // TODO refactor to remove cyclical dependency between kap-query and kap-sparder
        String queryEngineClazz = System.getProperty("kylin-query-engine", "io.kyligence.kap.query.runtime.SparkEngine");
        try {
            QueryEngine queryEngine = (QueryEngine) Class.forName(queryEngineClazz).newInstance();
            return queryEngine.compute(dataContext, rel.getInput(0));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * match cubes and rewrite relNodes to fit the cube
     * @param rel
     */
    private void selectRealizationAndRewrite(RelNode rel) {
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN BEFORE OLAPImplementor", rel);
        QueryContext.current().record("end_plan");

        QueryContext.current().setWithoutSyntaxError(true);
        List<OLAPContext> contexts = QueryContextCutter.selectRealization(rel,
                BackdoorToggles.getIsQueryFromAutoModeling());
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REALIZATION IS SET", rel);

        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(rel, rel.getInput(0));
        QueryContext.current().setCalcitePlan(rel.copy(rel.getTraitSet(), rel.getInputs()));
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REWRITE", rel);

        QueryContext.current().setIsSparderUsed(true);
        KapContext.setKapRel((KapRel) rel.getInput(0));
        KapContext.setRowType(rel.getRowType());

        QueryContext.current().record("end_rewrite");
    }

}
