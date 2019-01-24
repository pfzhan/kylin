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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.query.schema.KylinSqlValidator;

/**
 * converter that parse, validate sql and convert to relNodes
 */
public class SQLConverter {

    private final SqlParser.Config parserConfig;
    private final Prepare.CatalogReader catalogReader;
    private final SqlValidator validator;
    private final SqlToRelConverter sqlToRelConverter;
    private final CalciteConnectionConfig connectionConfig;

    public SQLConverter(CalciteConnectionConfig connectionConfig, RelOptPlanner planner, Prepare.CatalogReader catalogReader) {
        this.connectionConfig = connectionConfig;
        this.catalogReader = catalogReader;
        parserConfig = SqlParser.configBuilder().setQuotedCasing(connectionConfig.quotedCasing())
                .setUnquotedCasing(connectionConfig.unquotedCasing()).setQuoting(connectionConfig.quoting())
                .setConformance(connectionConfig.conformance()).setCaseSensitive(connectionConfig.caseSensitive())
                .build();

        validator = createValidator(connectionConfig, catalogReader);

        sqlToRelConverter = createSqlToRelConverter(planner, validator, catalogReader);
    }

    private SqlValidator createValidator(CalciteConnectionConfig connectionConfig,
            Prepare.CatalogReader catalogReader) {
        final SqlOperatorTable opTab0 = connectionConfig.fun(SqlOperatorTable.class,
                ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(), catalogReader));
        SqlValidator sqlValidator = new KylinSqlValidator((SqlValidatorImpl) SqlValidatorUtil.newValidator(
                opTab0,
                catalogReader,
                javaTypeFactory(),
                connectionConfig.conformance()));
        sqlValidator.setIdentifierExpansion(true);
        sqlValidator.setDefaultNullCollation(connectionConfig.defaultNullCollation());
        return sqlValidator;
    }

    private SqlToRelConverter createSqlToRelConverter(RelOptPlanner planner, SqlValidator sqlValidator, Prepare.CatalogReader catalogReader) {
        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withTrimUnusedFields(true)
                .withExpand(Prepare.THREAD_EXPAND.get()).withExplain(false).build();

        final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(javaTypeFactory()));

        return new SqlToRelConverter(null, sqlValidator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
    }

    public RelDataTypeSystem relDataTypeSystem() {
        return new KylinRelDataTypeSystem();
    }

    public JavaTypeFactory javaTypeFactory() {
        return new JavaTypeFactoryImpl(relDataTypeSystem());
    }

    /**
     * parse, validate and convert sql into RelNodes
     * Note that the output relNodes are not optimized
     * @param sql
     * @return
     * @throws SqlParseException
     */
    public RelRoot convertSqlToRelNode(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSQL(sql);

        return convertToRelNode(sqlNode);
    }

    public SqlNode parseSQL(String sql) throws SqlParseException {
        return SqlParser.create(sql, parserConfig).parseQuery();
    }

    public RelRoot convertToRelNode(SqlNode sqlNode) {
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, true);

        if (connectionConfig.forceDecorrelate()) {
            root = root.withRel(sqlToRelConverter.decorrelate(sqlNode, root.rel));
        }

        /* OVERRIDE POINT */
        // https://github.com/Kyligence/KAP/issues/10964
        RelNode rel = root.rel;
        if (connectionConfig.projectUnderRelRoot() && !root.isRefTrivial()) {
            final List<RexNode> projects = new ArrayList<>();
            final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            for (int field : Pair.left(root.fields)) {
                projects.add(rexBuilder.makeInputRef(rel, field));
            }
            LogicalProject project = LogicalProject.create(root.rel, projects, root.validatedRowType);
            //RelCollation must be cleared,
            //otherwise, relRoot's top rel will be reset to LogicalSort
            //in org.apache.calcite.tools.Programs#standard's program1
            root = new RelRoot(project, root.validatedRowType,
                    root.kind, root.fields,  RelCollations.EMPTY);
        }

        return root;
    }
}
