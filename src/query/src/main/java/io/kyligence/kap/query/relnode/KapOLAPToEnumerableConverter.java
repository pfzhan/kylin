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
package io.kyligence.kap.query.relnode;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPToEnumerableConverter;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.query.exec.SparderMethod;
import io.kyligence.kap.query.util.QueryContextCutter;

/**
 * If you're renaming this class, please keep it ending with OLAPToEnumerableConverter
 * see org.apache.calcite.plan.OLAPRelMdRowCount#shouldIntercept(org.apache.calcite.rel.RelNode)
 */
public class KapOLAPToEnumerableConverter extends OLAPToEnumerableConverter implements EnumerableRel, IKeepNames {

    private static final String SPARDER_CALL_METHOD_NAME = "enumerable";

    public KapOLAPToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KapOLAPToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public Result implement(EnumerableRelImplementor enumImplementor, Prefer pref) {
        //        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN BEFORE OLAPImplementor", this);

        QueryContext.current().setWithoutSyntaxError(true);
        List<OLAPContext> contexts = QueryContextCutter.selectRealization(this,
                BackdoorToggles.getIsQueryFromAutoModeling());
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REALIZATION IS SET", this);

        // identify realization for each context
        doAccessControl(contexts, (KapRel) getInput());
        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(this, getInput());
        QueryContext.current().setCalcitePlan(this.copy(getTraitSet(), getInputs()));

        boolean sparderEnabled = KapConfig.getInstanceFromEnv().isSparderEnabled();
        if (!sparderEnabled) {
            QueryContext.current().setIsSparderUsed(false);
            OLAPRel.JavaImplementor impl = new OLAPRel.JavaImplementor(enumImplementor);
            EnumerableRel inputAsEnum = impl.createEnumerable((OLAPRel) getInput());
            this.replaceInput(0, inputAsEnum);
            return impl.visitChild(this, 0, inputAsEnum, pref);
        } else {
            QueryContext.current().setIsSparderUsed(true);
            final PhysType physType = PhysTypeImpl.of(enumImplementor.getTypeFactory(), getRowType(),
                    pref.preferCustom());
            final BlockBuilder list = new BlockBuilder();

            KapContext.setKapRel((KapRel) getInput());
            KapContext.setRowType(getRowType());
            if (QueryContext.current().isAsyncQuery()) {
                Expression enumerable = list.append(SPARDER_CALL_METHOD_NAME,
                        Expressions.call(SparderMethod.ASYNC_RESULT.method, enumImplementor.getRootExpression()));
                list.add(Expressions.return_(null, enumerable));
                return enumImplementor.result(physType, list.toBlock());
            }
            if (physType.getFormat() == JavaRowFormat.SCALAR) {
                Expression enumerable = list.append(SPARDER_CALL_METHOD_NAME,
                        Expressions.call(SparderMethod.COLLECT_SCALAR.method, enumImplementor.getRootExpression()));
                list.add(Expressions.return_(null, enumerable));
            } else {
                Expression enumerable = list.append(SPARDER_CALL_METHOD_NAME,
                        Expressions.call(SparderMethod.COLLECT.method, enumImplementor.getRootExpression()));
                list.add(Expressions.return_(null, enumerable));
            }
            return enumImplementor.result(physType, list.toBlock());
        }
    }
}
