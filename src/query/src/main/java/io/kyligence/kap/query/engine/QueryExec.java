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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
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
import io.kyligence.kap.query.engine.meta.KECalciteConfig;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;

/**
 * entrance for query execution
 */
public class QueryExec {

    private final KylinConfig kylinConfig;
    private final CalciteConnectionConfig config;
    private final RelOptPlanner planner;
    private final KapProjectSchemaFactory schemaFactory;
    private final Prepare.CatalogReader catalogReader;
    private final SQLConverter sqlConverter;
    private final QueryOptimizer queryOptimizer;
    private final SimpleDataContext dataContext;

    public QueryExec(String project, KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        config = KECalciteConfig.fromKapConfig(kylinConfig);
        schemaFactory = new KapProjectSchemaFactory(project, kylinConfig);
        catalogReader = createCatalogReader(config, schemaFactory);
        planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(config);
        sqlConverter = new SQLConverter(config, planner, catalogReader);
        dataContext = createDataContext();
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
            Prepare.CatalogReader.THREAD_LOCAL.set(catalogReader);

            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);

            RelRoot optimizedRelRoot = queryOptimizer.optimize(relRoot);

            return executeQueryPlan(optimizedRelRoot.rel);
        } catch (SqlParseException e) {
            // some special message for parsing error... to be compatible with avatica's error msg
            throw newSqlException(sql, "parse failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw newSqlException(sql, e.getMessage(), e);
        } finally {
            Prepare.CatalogReader.THREAD_LOCAL.remove();
        }
    }

    /**
     * get metadata of columns of the query result
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<StructField> getColumnMetaData(String sql) throws SQLException {
        try {
            Prepare.CatalogReader.THREAD_LOCAL.set(catalogReader);

            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);

            return new MetaDataExtractor().getColumnMetadata(relRoot.rel);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            Prepare.CatalogReader.THREAD_LOCAL.remove();
        }
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
    private QueryResult executeQueryPlan(RelNode rel) {
        QueryPlanExec planExec;

        if (!KapConfig.wrap(kylinConfig).isSparderEnabled() ||
                (KapConfig.wrap(kylinConfig).runConstantQueryLocally() && isConstantQuery(rel))) {
            planExec = new CalciteQueryPlanExec(); // if sparder is not enabled, or the sql can run locally, use the calcite engine
        } else {
            planExec = new SparderQueryPlanExec();
        }
        return new QueryResult(planExec.execute(rel, dataContext),
                new MetaDataExtractor().getColumnMetadata(rel));
    }

    private Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig, KapProjectSchemaFactory schemaFactory) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(schemaFactory.createProjectRootSchema(), Collections.singletonList(schemaFactory.getDefaultSchema()),
                javaTypeFactory, connectionConfig);
    }

    private SimpleDataContext createDataContext() {
        return new SimpleDataContext(
                schemaFactory.createProjectRootSchema().plus(),
                sqlConverter.javaTypeFactory(),
                kylinConfig);
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

    private SQLException newSqlException(String sql, String msg, Throwable e) {
        return new SQLException("Error while executing SQL \"" + sql + "\": " + msg, e);
    }
}
