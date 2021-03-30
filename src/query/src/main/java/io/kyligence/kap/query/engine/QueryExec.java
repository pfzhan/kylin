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

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.query.engine.mask.QueryResultMasks;
import io.kyligence.kap.query.util.CalcitePlanRouterVisitor;
import io.kyligence.kap.query.util.HepUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.ReadFsSwitch;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.engine.exec.calcite.CalciteQueryPlanExec;
import io.kyligence.kap.query.engine.exec.sparder.SparderQueryPlanExec;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;

/**
 * Entrance for query execution
 */
public class QueryExec {
    private static final Logger logger = LoggerFactory.getLogger(QueryExec.class);

    private final KylinConfig kylinConfig;
    private final KECalciteConfig config;
    private final RelOptPlanner planner;
    private final ProjectSchemaFactory schemaFactory;
    private final Prepare.CatalogReader catalogReader;
    private final SQLConverter sqlConverter;
    private final QueryOptimizer queryOptimizer;
    private final SimpleDataContext dataContext;
    private final boolean allowAlternativeQueryPlan;

    public QueryExec(String project, KylinConfig kylinConfig, boolean allowAlternativeQueryPlan) {
        this.kylinConfig = kylinConfig;
        config = KECalciteConfig.fromKapConfig(kylinConfig);
        schemaFactory = new ProjectSchemaFactory(project, kylinConfig);
        CalciteSchema rootSchema = schemaFactory.createProjectRootSchema();
        String defaultSchemaName = schemaFactory.getDefaultSchema();
        catalogReader = createCatalogReader(config, rootSchema, defaultSchemaName);
        planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(config);
        sqlConverter = new SQLConverter(config, planner, catalogReader);
        dataContext = createDataContext(rootSchema);
        planner.setExecutor(new RexExecutorImpl(dataContext));
        queryOptimizer = new QueryOptimizer(planner);
        this.allowAlternativeQueryPlan = allowAlternativeQueryPlan;
    }

    public QueryExec(String project, KylinConfig kylinConfig) {
        this(project, kylinConfig, false);
    }

    public void plannerRemoveRules(List<RelOptRule> rules) {
        for (RelOptRule rule : rules) {
            planner.removeRule(rule);
        }
    }

    public void plannerAddRules(List<RelOptRule> rules) {
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
    }

    /**
     * parse, optimize sql and execute the sql physically
     * @param sql
     * @return query result data with column infos
     */
    public QueryResult executeQuery(String sql) throws SQLException {
        magicDirts(sql);
        try {
            beforeQuery();
            QueryContext.currentTrace().startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
            QueryContext.current().record("end calcite convert sql to relnode");
            RelNode node = queryOptimizer.optimize(relRoot).rel;
            QueryContext.current().record("end calcite optimize");

            List<StructField> resultFields = RelColumnMetaDataExtractor.getColumnMetadata(relRoot.validatedRowType);
            if (resultFields.isEmpty()) { // result fields size may be 0 because of ACL controls and should return immediately
                QueryContext.fillEmptyResultSetMetrics();
                return new QueryResult();
            }

            if (kylinConfig.getEmptyResultForSelectStar() && sql.toLowerCase(Locale.ROOT).matches("^select\\s+\\*\\p{all}*")
                    && !QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                return new QueryResult(Lists.newArrayList(), resultFields);
            }

            QueryResultMasks.setRootRelNode(node);
            return new QueryResult(executeQueryPlan(postOptimize(node)), resultFields);
        } catch (SqlParseException e) {
            // some special message for parsing error... to be compatible with avatica's error msg
            throw newSqlException(sql, "parse failed: " + e.getMessage(), e);
        } catch (Exception e) {
            // retry query if switched to backup read FS
            if (ReadFsSwitch.turnOnSwitcherIfBackupFsAllowed(e,
                    KapConfig.wrap(kylinConfig).getSwitchBackupFsExceptionAllowString())) {
                logger.info("Retry sql after hitting allowed exception and turn on backup read FS", e);
                return executeQuery(sql);
            }
            throw newSqlException(sql, e.getMessage(), e);
        } finally {
            afterQuery();
        }
    }

    private void magicDirts(String sql) {
        if (sql.contains("ReadFsSwitch.turnOnBackupFsWhile")) {
            ReadFsSwitch.turnOnBackupFsWhile();
        }
    }

    /**
     * Separating <code>parseAndOptimize</code> and <code>postOptimize</code> is only for UT test.
     */
    @VisibleForTesting
    public RelNode parseAndOptimize(String sql) throws SqlParseException {
        RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
        return queryOptimizer.optimize(relRoot).rel;
    }

    /**
     * Apply post optimization rules and produce a list of alternative transformed nodes
     */
    private List<RelNode> postOptimize(RelNode node) {
        Collection<RelOptRule> postOptRules = new LinkedHashSet<>();
        if (kylinConfig.isConvertSumExpressionEnabled()) {
            postOptRules.addAll(HepUtils.SumExprRules);
        }
        if (kylinConfig.isConvertCountDistinctExpressionEnabled()) {
            postOptRules.addAll(HepUtils.CountDistinctExprRules);
        }

        if (!postOptRules.isEmpty()) {
            RelNode transformed = HepUtils.runRuleCollection(node, postOptRules, false);
            if (transformed != node && allowAlternativeQueryPlan) {
                return Lists.newArrayList(transformed, node);
            } else {
                return Lists.newArrayList(transformed);
            }
        }
        return Lists.newArrayList(node);
    }

    @VisibleForTesting
    public RelRoot sqlToRelRoot(String sql) throws SqlParseException {
        return sqlConverter.convertSqlToRelNode(sql);
    }

    @VisibleForTesting
    public RelRoot optimize(RelRoot relRoot) {
        return queryOptimizer.optimize(relRoot);
    }

    /**
     * get metadata of columns of the query result
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<StructField> getColumnMetaData(String sql) throws SQLException {
        try {
            beforeQuery();

            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);

            return RelColumnMetaDataExtractor.getColumnMetadata(relRoot.validatedRowType);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            afterQuery();
        }
    }

    private void beforeQuery() {
        Prepare.CatalogReader.THREAD_LOCAL.set(catalogReader);
        KECalciteConfig.THREAD_LOCAL.set(config);
    }

    private void afterQuery() {
        Prepare.CatalogReader.THREAD_LOCAL.remove();
        KECalciteConfig.THREAD_LOCAL.remove();
    }

    public void setContextVar(String name, Object val) {
        dataContext.putContextVar(name, val);
    }

    /**
     * set prepare params
     * @param idx 0-based index
     * @param val
     */
    public void setPrepareParam(int idx, Object val) {
        dataContext.setPrepareParam(idx, val);
    }

    /**
     * get default schema used in this query
     * @return
     */
    public String getSchema() {
        return schemaFactory.getDefaultSchema();
    }

    /**
     * execute query plan physically
     * @param rels list of alternative relNodes to execute.
     *             relNodes will be executed one by one and return on the first successful execution
     * @return
     */
    private List<List<String>> executeQueryPlan(List<RelNode> rels) {
        boolean routeToCalcite = routeToCalciteEngine(rels.get(0));
        dataContext.setContentQuery(routeToCalcite);
        if (!QueryContext.current().getQueryTagInfo().isAsyncQuery()
                && KapConfig.wrap(kylinConfig).runConstantQueryLocally() && routeToCalcite) {
            return new CalciteQueryPlanExec().execute(rels.get(0), dataContext); // if sparder is not enabled, or the sql can run locally, use the calcite engine
        } else {
            NoRealizationFoundException lastException = null;
            for (RelNode rel : rels) {
                try {
                    return new SparderQueryPlanExec().execute(rel, dataContext);
                } catch (NoRealizationFoundException e) {
                    lastException = e;
                }
            }
            assert lastException != null;
            throw lastException;
        }
    }

    private Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig,
            CalciteSchema rootSchema, String defaultSchemaName) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(rootSchema, Collections.singletonList(defaultSchemaName), javaTypeFactory,
                connectionConfig);
    }

    private SimpleDataContext createDataContext(CalciteSchema rootSchema) {
        return new SimpleDataContext(rootSchema.plus(), sqlConverter.javaTypeFactory(), kylinConfig);
    }

    private boolean routeToCalciteEngine(RelNode rel) {
        return isConstantQuery(rel) && isCalciteEngineCapable(rel);
    }

    /**
     * search rel node tree to see if there is any table scan node
     * @param rel
     * @return
     */
    private boolean isConstantQuery(RelNode rel) {
        if (TableScan.class.isAssignableFrom(rel.getClass())) {
            return false;
        }
        for (RelNode input : rel.getInputs()) {
            if (!isConstantQuery(input)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Calcite is not capable for some constant queries, need to route to Sparder
     *
     * @param rel
     * @return
     */
    private boolean isCalciteEngineCapable(RelNode rel) {
        if (rel instanceof Project) {
            Project projectRelNode = (Project) rel;
            if (projectRelNode.getChildExps().stream().filter(pRelNode -> pRelNode instanceof RexCall)
                    .anyMatch(pRelNode -> pRelNode.accept(new CalcitePlanRouterVisitor()))) {
                return false;
            }
        }

        return rel.getInputs().stream().allMatch(this::isCalciteEngineCapable);
    }

    private SQLException newSqlException(String sql, String msg, Throwable e) {
        return new SQLException("Error while executing SQL \"" + sql + "\": " + msg, e);
    }
}
