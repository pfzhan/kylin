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

package io.kyligence.kap.query.engine;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinUnionTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.optrule.AggregateMultipleExpandRule;
import org.apache.kylin.query.optrule.AggregateProjectReduceRule;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import io.kyligence.kap.query.engine.meta.PlannerContext;
import io.kyligence.kap.query.optrule.CorrReduceFunctionRule;
import io.kyligence.kap.query.optrule.KAPValuesRule;
import io.kyligence.kap.query.optrule.KapAggregateReduceFunctionsRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapFilterJoinRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapLimitRule;
import io.kyligence.kap.query.optrule.KapMinusRule;
import io.kyligence.kap.query.optrule.KapModelViewRule;
import io.kyligence.kap.query.optrule.KapOLAPToEnumerableConverterRule;
import io.kyligence.kap.query.optrule.KapProjectJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapProjectMergeRule;
import io.kyligence.kap.query.optrule.KapProjectRule;
import io.kyligence.kap.query.optrule.KapSortRule;
import io.kyligence.kap.query.optrule.KapUnionRule;
import io.kyligence.kap.query.optrule.KapWindowRule;
import io.kyligence.kap.query.optrule.RightJoinToLeftJoinRule;
import io.kyligence.kap.query.optrule.SumConstantConvertRule;

/**
 * factory that create optimizers and register opt rules
 * TODO sort and register only necessary rules
 */
public class PlannerFactory {

    public static final List<RelOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

    private static final List<RelOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    AggregateStarTableRule.INSTANCE,
                    AggregateStarTableRule.INSTANCE2,
                    TableScanRule.INSTANCE,
                    ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    KapAggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    ProjectWindowTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE,
                    SortJoinTransposeRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE);

    private final KylinConfig kylinConfig;

    public PlannerFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public VolcanoPlanner createVolcanoPlanner(CalciteConnectionConfig connectionConfig) {
        VolcanoPlanner planner = new VolcanoPlanner(new PlannerContext(connectionConfig));
        registerDefaultRules(planner);
        registerCustomRules(planner);
        return planner;
    }

    private void registerDefaultRules(VolcanoPlanner planner) {
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(planner);
        for (RelOptRule rule : DEFAULT_RULES) {
            planner.addRule(rule);
        }
        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(ProjectTableScanRule.INSTANCE);
        planner.addRule(ProjectTableScanRule.INTERPRETER);
        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.addRule(rule);
        }
        planner.addRule(EnumerableInterpreterRule.INSTANCE);

        for (RelOptRule rule : StreamRules.RULES) {
            planner.addRule(rule);
        }
    }

    private void registerCustomRules(VolcanoPlanner planner) {
        // force clear the query context before traversal relational operators
        OLAPContext.clearThreadLocalContexts();
        // register OLAP rules
        //        addRules(planner, kylinConfig.getCalciteAddRule());
        // register OLAP rules
        planner.addRule(KapOLAPToEnumerableConverterRule.INSTANCE);
        planner.addRule(KapFilterRule.INSTANCE);
        planner.addRule(KapProjectRule.INSTANCE);
        planner.addRule(KapAggregateRule.INSTANCE);
        planner.addRule(selectJoinRuleByConfig());
        planner.addRule(KapLimitRule.INSTANCE);
        planner.addRule(KapSortRule.INSTANCE);
        planner.addRule(KapUnionRule.INSTANCE);
        planner.addRule(KapWindowRule.INSTANCE);
        planner.addRule(KAPValuesRule.INSTANCE);
        planner.addRule(KapMinusRule.INSTANCE);
        planner.addRule(KapModelViewRule.INSTANCE);
        planner.removeRule(ProjectMergeRule.INSTANCE);
        planner.addRule(KapProjectMergeRule.INSTANCE);

        // Support translate the grouping aggregate into union of simple aggregates
        // if it's the auto-modeling dry run, then do not add the CorrReduceFunctionRule
        // Todo cherry-pick CORR measure
        //        if (!KapConfig.getInstanceFromEnv().getSkipCorrReduceRule()) {
        //            planner.addRule(CorrReduceFunctionRule.INSTANCE);
        //        }
        if (KapConfig.getInstanceFromEnv().splitGroupSetsIntoUnion()) {
            planner.addRule(AggregateMultipleExpandRule.INSTANCE);
        }
        planner.addRule(AggregateProjectReduceRule.INSTANCE);

        if (!kylinConfig.isConvertSumExpressionEnabled()) {
            planner.addRule(SumConstantConvertRule.INSTANCE);
        }

        // CalcitePrepareImpl.CONSTANT_REDUCTION_RULES
        if (kylinConfig.isReduceExpressionsRulesEnabled()) {
            planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
            planner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);
            planner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
            planner.addRule(ReduceExpressionsRule.JOIN_INSTANCE);
        }
        // the ValuesReduceRule breaks query test somehow...
        //        planner.addRule(ValuesReduceRule.FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_INSTANCE);

        removeRules(planner, kylinConfig.getCalciteRemoveRule());
        if (!kylinConfig.isEnumerableRulesEnabled()) {
            for (RelOptRule rule : CalcitePrepareImpl.ENUMERABLE_RULES) {
                planner.removeRule(rule);
            }
        }
        // since join is the entry point, we can't push filter past join
        planner.removeRule(FilterJoinRule.FILTER_ON_JOIN);
        planner.removeRule(FilterJoinRule.JOIN);
        planner.addRule(KapFilterJoinRule.KAP_FILTER_ON_JOIN_JOIN);
        planner.addRule(KapFilterJoinRule.KAP_FILTER_ON_JOIN_SCAN);
        // since we don't have statistic of table, the optimization of join is too cost
        planner.removeRule(JoinCommuteRule.INSTANCE);
        planner.removeRule(JoinPushThroughJoinRule.LEFT);
        planner.removeRule(JoinPushThroughJoinRule.RIGHT);

        // keep tree structure like filter -> aggregation -> project -> join/table scan, implementOLAP() rely on this tree pattern
        //        planner.removeRule(AggregateJoinTransposeRule.INSTANCE);
        planner.removeRule(AggregateProjectMergeRule.INSTANCE);
        planner.removeRule(FilterProjectTransposeRule.INSTANCE);
        planner.removeRule(SortJoinTransposeRule.INSTANCE);
        planner.removeRule(JoinPushExpressionsRule.INSTANCE);
        planner.removeRule(SortUnionTransposeRule.INSTANCE);
        planner.removeRule(JoinUnionTransposeRule.LEFT_UNION);
        planner.removeRule(JoinUnionTransposeRule.RIGHT_UNION);
        planner.removeRule(AggregateUnionTransposeRule.INSTANCE);
        planner.removeRule(DateRangeRules.FILTER_INSTANCE);
        planner.removeRule(SemiJoinRule.JOIN);
        planner.removeRule(SemiJoinRule.PROJECT);
        // distinct count will be split into a separated query that is joined with the left query
        planner.removeRule(AggregateExpandDistinctAggregatesRule.INSTANCE);

        // see Dec 26th email @ http://mail-archives.apache.org/mod_mbox/calcite-dev/201412.mbox/browser
        planner.removeRule(AbstractConverter.ExpandConversionRule.INSTANCE);
        // convert all right joins to left join since we only support left joins in model
        planner.addRule(RightJoinToLeftJoinRule.INSTANCE);
        // UnionMergeRule may slow volcano planner optimization on large number of union clause
        // see KAP#16036
        planner.removeRule(UnionMergeRule.INSTANCE);

        planner.addRule(KapProjectJoinTransposeRule.INSTANCE);
        planner.removeRule(ProjectRemoveRule.INSTANCE);

        // skip corr expandsion during model suggestion
        if (!KylinConfig.getInstanceFromEnv().getSkipCorrReduceRule()) {
            planner.addRule(CorrReduceFunctionRule.INSTANCE);
        }
    }

    private ConverterRule selectJoinRuleByConfig() {
        return kylinConfig.isQueryNonEquiJoinModelEnabled() && (!BackdoorToggles.getIsQueryFromAutoModeling()
                || BackdoorToggles.getIsQueryNonEquiJoinModelEnabled() || KylinConfig.getInstanceFromEnv().isUTEnv())
                        ? KapJoinRule.NON_EQUI_INSTANCE
                        : KapJoinRule.INSTANCE;
    }

    protected void removeRules(final RelOptPlanner planner, List<String> rules) {
        modifyRules(rules, new Function<RelOptRule, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable RelOptRule input) {
                planner.removeRule(input);
                return null;
            }
        });
    }

    private void modifyRules(List<String> rules, Function<RelOptRule, Void> func) {
        for (String rule : rules) {
            if (StringUtils.isEmpty(rule)) {
                continue;
            }
            String[] split = rule.split("#");
            if (split.length != 2) {
                throw new RuntimeException("Customized Rule should be in format <RuleClassName>#<FieldName>");
            }
            String clazz = split[0];
            String field = split[1];
            try {
                func.apply((RelOptRule) Class.forName(clazz).getDeclaredField(field).get(null));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
