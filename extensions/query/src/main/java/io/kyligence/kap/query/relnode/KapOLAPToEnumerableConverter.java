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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPToEnumerableConverter;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.kylin.query.security.QueryInterceptor;
import org.apache.kylin.query.security.QueryInterceptorUtil;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.ext.classloader.ClassLoaderUtils;
import io.kyligence.kap.metadata.model.IKapStorageAware;
import io.kyligence.kap.query.runtime.SparderMethod;

/**
 * If you're renaming this class, please keep it ending with OLAPToEnumerableConverter
 * see org.apache.calcite.plan.OLAPRelMdRowCount#shouldIntercept(org.apache.calcite.rel.RelNode)
 */
public class KapOLAPToEnumerableConverter extends OLAPToEnumerableConverter implements EnumerableRel {

    private static final Logger logger = LoggerFactory.getLogger(KapOLAPToEnumerableConverter.class);

    public KapOLAPToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KapOLAPToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    private void dumpCalcitePlan(String msg) {
        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println(msg);
            System.out.println(dumpPlan);
        }
    }

    @Override
    public Result implement(EnumerableRelImplementor enumImplementor, Prefer pref) {
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        dumpCalcitePlan("EXECUTION PLAN BEFORE OLAPImplementor");

        // post-order travel children
        OLAPRel.OLAPImplementor KAPImplementor = new OLAPRel.OLAPImplementor();
        KAPImplementor.visitChild(getInput(), this);

        dumpCalcitePlan("EXECUTION PLAN AFTER OLAPCONTEXT IS SET");

        // identify model
        List<OLAPContext> contexts = listContextsHavingScan();

        // intercept query
        List<QueryInterceptor> intercepts = QueryInterceptorUtil.getQueryInterceptors();
        for (QueryInterceptor intercept : intercepts) {
            intercept.intercept(contexts);
        }

        RealizationChooser.selectRealization(contexts);

        dumpCalcitePlan("EXECUTION PLAN AFTER REALIZATION IS SET");

        // identify realization for each context
        doAccessControl(contexts);
        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(this, getInput());

        boolean sparderEnabled = KapConfig.getInstanceFromEnv().isSparderEnabled();
        if (sparderEnabled) {
            sparderEnabled = isSparderAppliable(contexts);
            logger.info("sparder is enabled");
        }

        if (!sparderEnabled) {
            OLAPRel.JavaImplementor impl = new OLAPRel.JavaImplementor(enumImplementor);
            EnumerableRel inputAsEnum = impl.createEnumerable((OLAPRel) getInput());
            this.replaceInput(0, inputAsEnum);
            return impl.visitChild(this, 0, inputAsEnum, pref);
        } else {
            final PhysType physType = PhysTypeImpl.of(enumImplementor.getTypeFactory(), getRowType(),
                    pref.preferCustom());
            final BlockBuilder list = new BlockBuilder();

            KapContext.setKapRel((KapRel) getInput());
            KapContext.setRowType(getRowType());

            if (physType.getFormat() == JavaRowFormat.SCALAR) {
                Expression enumerable = list.append("enumerable",
                        Expressions.call(SparderMethod.COLLECT_SCALAR.method, enumImplementor.getRootExpression()));
                list.add(Expressions.return_(null, enumerable));
            } else {
                Expression enumerable = list.append("enumerable",
                        Expressions.call(SparderMethod.COLLECT.method, enumImplementor.getRootExpression()));
                list.add(Expressions.return_(null, enumerable));
            }
            return enumImplementor.result(physType, list.toBlock());
        }
    }

    private boolean isSparderAppliable(List<OLAPContext> contexts) {
        boolean sparderEnabled = true;
        boolean hasAgg = false;
        for (OLAPContext olapContext : contexts) {
            if (olapContext.aggregations.size() > 0) {
                hasAgg = true;
            }
            CubeInstance cube = null;
            if (olapContext.realization instanceof RawTableInstance) {
                sparderEnabled = false;
                break;
            }
            if (olapContext.realization instanceof CubeInstance) {
                if (!isCFAndStorageType(sparderEnabled, olapContext)) {
                    sparderEnabled = false;
                    break;
                }
            }
            //Hybrid
            if (olapContext.realization instanceof HybridInstance) {
                HybridInstance hybridInstance = (HybridInstance) olapContext.realization;
                IRealization[] realizations = hybridInstance.getRealizations();
                for (IRealization realization : realizations) {
                    if (realization instanceof RawTableInstance) {
                        sparderEnabled = false;
                        break;
                    }
                    if (olapContext.realization instanceof CubeInstance) {
                        if (!isCFAndStorageType(sparderEnabled, olapContext)) {
                            sparderEnabled = false;
                            break;
                        }
                    }

                }
            }
            if (!sparderEnabled) {
                break;
            }
            //window
            if (olapContext.hasWindow) {
                logger.debug("Current query cannot use Sparder due to window function");
                sparderEnabled = false;
                break;
            }

            //needAdvancedTupleFilling
            if (olapContext.involvedMeasure.size() > 0) {
                logger.debug("Current query cannot use Sparder due to unsupported needAdvancedTupleFilling measure: "
                        + olapContext.involvedMeasure);
                sparderEnabled = false;
                break;
            }

            for (SQLDigest.SQLCall sqlCall : olapContext.aggrSqlCalls) {
                if (sqlCall.function.equalsIgnoreCase("intersect_count")
                        || sqlCall.function.toUpperCase().contains("percentile".toUpperCase())) {
                    logger.debug(
                            "Current query cannot use Sparder due to unsupported udf function " + sqlCall.function);
                    sparderEnabled = false;
                    break;
                }
            }
        }
        if (!hasAgg) {
            sparderEnabled = false;
        }
        return sparderEnabled;
    }

    private boolean isCFAndStorageType(boolean sparderEnabled, OLAPContext olapContext) {
        CubeInstance cube;
        cube = (CubeInstance) olapContext.realization;
        CubeDesc cubeDesc = cube.getDescriptor();
        //CF
        if (cubeDesc.getHbaseMapping() != null
                && cubeDesc.getHbaseMapping().getColumnFamily().length != cubeDesc.getMeasures().size()) {
            logger.debug("Current query cannot use Sparder due to Column Family is used"
                    + olapContext.realization.getCanonicalName());
            sparderEnabled = false;
        }
        //storag type
        if (cube.getDescriptor().getStorageType() != IKapStorageAware.ID_SPLICE_PARQUET) {
            logger.debug("Current query cannot use Sparder due to unsupported storage type"
                    + cube.getDescriptor().getStorageType());
            sparderEnabled = false;
        }
        return sparderEnabled;
    }

}
