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
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import io.kyligence.kap.query.util.HepUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPlanner;
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
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;

import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.engine.exec.QueryPlanExec;
import io.kyligence.kap.query.engine.exec.calcite.CalciteQueryPlanExec;
import io.kyligence.kap.query.engine.exec.sparder.SparderQueryPlanExec;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;
import io.kyligence.kap.query.util.RexHasConstantUdfVisitor;

/**
 * Entrance for query execution
 */
public class QueryExec {

    private final KylinConfig kylinConfig;
    private final KECalciteConfig config;
    private final RelOptPlanner planner;
    private final ProjectSchemaFactory schemaFactory;
    private final Prepare.CatalogReader catalogReader;
    private final SQLConverter sqlConverter;
    private final QueryOptimizer queryOptimizer;
    private final SimpleDataContext dataContext;

    public QueryExec(String project, KylinConfig kylinConfig) {
        this(kylinConfig, new ProjectSchemaFactory(project, kylinConfig));
     }

     @VisibleForTesting
     public QueryExec(KylinConfig kylinConfig, ProjectSchemaFactory projectSchemaFactory){
         this.kylinConfig = kylinConfig;
         config = KECalciteConfig.fromKapConfig(kylinConfig);
         schemaFactory =  projectSchemaFactory;
         CalciteSchema rootSchema = schemaFactory.createProjectRootSchema();
         String defaultSchemaName = schemaFactory.getDefaultSchema();
         catalogReader = createCatalogReader(config, rootSchema, defaultSchemaName);
         planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(config);
         sqlConverter = new SQLConverter(config, planner, catalogReader);
         dataContext = createDataContext(rootSchema);
         planner.setExecutor(new RexExecutorImpl(dataContext));
         queryOptimizer = new QueryOptimizer(planner);
     }

    /**
     * parse, optimize sql and execute the sql physically
     * @param sql
     * @return query result data with column infos
     */
    public QueryResult executeQuery(String sql) throws SQLException {
        try {
            beforeQuery();         
            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
            RelNode node = queryOptimizer.optimize(relRoot).rel;
            return new QueryResult(executeQueryPlan(postOptimize(node)),
                    RelColumnMetaDataExtractor.getColumnMetadata(relRoot.validatedRowType));
        } catch (SqlParseException e) {
            // some special message for parsing error... to be compatible with avatica's error msg
            throw newSqlException(sql, "parse failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw newSqlException(sql, e.getMessage(), e);
        } finally {
            afterQuery();
        }
    }

    /**
     * Separating <code>parseAndOptimize</code> and <code>postOptimize</code> is only for UT test.
     */
    @VisibleForTesting
    public RelNode parseAndOptimize(String sql) throws SqlParseException {
        RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
        return  queryOptimizer.optimize(relRoot).rel;
    }

    @VisibleForTesting
    public RelNode postOptimize(RelNode node) {
        if (kylinConfig.isConvertSumExpressionEnabled()) {
            node = HepUtils.runRuleCollection(node, HepUtils.SumExprRule);
        }
        return node;
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
     * @param rel
     * @return
     */
    private List<List<String>> executeQueryPlan(RelNode rel) {
        QueryPlanExec planExec;

        if (!KapConfig.wrap(kylinConfig).isSparderEnabled()
                || (KapConfig.wrap(kylinConfig).runConstantQueryLocally() && isConstantQuery(rel))) {
            planExec = new CalciteQueryPlanExec(); // if sparder is not enabled, or the sql can run locally, use the calcite engine
        } else {
            planExec = new SparderQueryPlanExec();
        }
        return planExec.execute(rel, dataContext);
    }

    private Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig,
                                                      CalciteSchema rootSchema, String defaultSchemaName) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(rootSchema,
                Collections.singletonList(defaultSchemaName), javaTypeFactory, connectionConfig);
    }

    private SimpleDataContext createDataContext(CalciteSchema rootSchema) {
        return new SimpleDataContext(rootSchema.plus(), sqlConverter.javaTypeFactory(),
                kylinConfig);
    }

    private boolean isConstantQuery(RelNode rel) {
        return !hasTableScanNode(rel) && !hasNotConstantUDF(rel);
    }

    /**
     * search rel node tree to see if there is any table scan node
     * @param rel
     * @return
     */
    private boolean hasTableScanNode(RelNode rel) {
        if (TableScan.class.isAssignableFrom(rel.getClass())) {
            return true;
        }
        for (RelNode input : rel.getInputs()) {
            if (hasTableScanNode(input)) {
                return true;
            }
        }

        return false;
    }

    /**
     * In constant query,
     * visit RelNode tree to see if any UDF implement {@link org.apache.calcite.sql.type.NotConstant}
     *
     * @param rel
     * @return true if any UDF implement {@link org.apache.calcite.sql.type.NotConstant},otherwise false
     */
    private boolean hasNotConstantUDF(RelNode rel) {

        if (rel instanceof Project) {
            Project projectRelNode = (Project) rel;
            if (projectRelNode.getChildExps().stream().filter(pRelNode -> pRelNode instanceof RexCall)
                    .anyMatch(pRelNode -> pRelNode.accept(new RexHasConstantUdfVisitor()))) {
                return true;
            }
        }

        return rel.getInputs().stream().anyMatch(this::hasNotConstantUDF);
    }

    private SQLException newSqlException(String sql, String msg, Throwable e) {
        return new SQLException("Error while executing SQL \"" + sql + "\": " + msg, e);
    }
}
