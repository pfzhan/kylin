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

package io.kyligence.kap.query.engine.view;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModel;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A schema with no concrete tables
 * view tables will be registered later and accessed via
 * org.apache.calcite.jdbc.CalciteSchema#getTableBasedOnNullaryFunction(java.lang.String, boolean)
 */
public class ViewSchema extends AbstractSchema {

    private final String schemaName;
    private final ViewAnalyzer analyzer;

    public ViewSchema(String schemaName, ViewAnalyzer analyzer) {
        this.schemaName = schemaName;
        this.analyzer = analyzer;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>();
    }

    public void addModel(SchemaPlus schemaPlus, NDataModel model) {
        schemaPlus.add(model.getAlias(), createViewMacro(model));
    }

    private LazyParsedViewTableMacro createViewMacro(NDataModel model) {
        String viewSQL = new ModelViewGenerator(model).generateViewSQL();
        List<String> schemaPath = Lists.newArrayList(schemaName);
        List<String> viewPath = Lists.newArrayList(schemaName, model.getAlias());
        return new LazyParsedViewTableMacro(() -> analyzeView(viewSQL), viewSQL, schemaPath, viewPath);
    }

    public CalcitePrepare.AnalyzeViewResult analyzeView(String sql) {
        try {
            return analyzer.analyzeView(sql);
        } catch (SqlParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR, e);
        }
    }

    public static class LazyParsedViewTableMacro extends ViewTableMacro {
        private static boolean modifiable = false;
        // SqlValidatorImpl.validateSelect may call apply() many times
        // cache the parsed result here
        private CalcitePrepare.AnalyzeViewResult parsed;
        private final Supplier<CalcitePrepare.AnalyzeViewResult> parseFunction;

        public LazyParsedViewTableMacro(Supplier<CalcitePrepare.AnalyzeViewResult> parseFunction, String viewSql,
                                        List<String> schemaPath, List<String> viewPath) {
            super(null, viewSql, schemaPath, viewPath, modifiable);
            this.parseFunction = parseFunction;
        }

        @Override
        public TranslatableTable apply(List<Object> arguments) {
            return this.viewTable(getParsed(), viewSql, schemaPath, viewPath);
        }

        private CalcitePrepare.AnalyzeViewResult getParsed() {
            if (parsed == null) {
                parsed = parseFunction.get();
            }
            return parsed;
        }
    }

}
