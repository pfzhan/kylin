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
import io.kyligence.kap.guava20.shaded.common.cache.Cache;
import io.kyligence.kap.guava20.shaded.common.cache.CacheBuilder;
import io.kyligence.kap.metadata.model.NDataModel;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A schema with no concrete tables
 * view tables will be registered later and accessed via
 * org.apache.calcite.jdbc.CalciteSchema#getTableBasedOnNullaryFunction(java.lang.String, boolean)
 */
public class ViewSchema extends AbstractSchema {

    // map model id to view macros
    // saving the view sql creation and analyze time
    private static final Cache<String, ParsedViewTableMacro> viewMacroCache = CacheBuilder.newBuilder()
            .maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).build();

    private final String schemaName;

    public ViewSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>();
    }

    public void addModel(SchemaPlus schemaPlus, NDataModel model) {
        ParsedViewTableMacro macro = viewMacroCache.getIfPresent(model.getId());
        if (macro == null || macro.getLastModifiedAt() < model.getLastModified()) {
            macro = createViewMacro(schemaPlus, model);
            viewMacroCache.put(model.getId(), macro);
        }

        schemaPlus.add(model.getAlias(), macro);
    }

    private ParsedViewTableMacro createViewMacro(SchemaPlus schemaPlus, NDataModel model) {
        String viewSQL = new ModelViewGenerator(model).generateViewSQL();
        List<String> schemaPath = Lists.newArrayList(schemaName);
        List<String> viewPath = Lists.newArrayList(schemaName, model.getAlias());
        CalcitePrepare.AnalyzeViewResult parsed = analyzeView(schemaPlus, viewSQL, schemaPath, viewPath);
        return new ParsedViewTableMacro(parsed, viewSQL, schemaPath, viewPath, model.getLastModified());
    }

    // TODO try avoid using CalciteConnection
    private static CalcitePrepare.AnalyzeViewResult analyzeView(
            SchemaPlus schema, String viewSql, List<String> schemaPath, List<String> viewPath) {
        CalciteConnection connection = MaterializedViewTable.MATERIALIZATION_CONNECTION;
        return Schemas.analyzeView(connection, CalciteSchema.from(schema), schemaPath, viewSql, viewPath, false);
    }

    public static class ParsedViewTableMacro extends ViewTableMacro {
        private final long lastModifiedAt;
        private static boolean modifiable = false;
        // SqlValidatorImpl.validateSelect may call apply() many times
        // cache the parsed result here
        private CalcitePrepare.AnalyzeViewResult parsed;

        public ParsedViewTableMacro(CalcitePrepare.AnalyzeViewResult parsed, String viewSql,
                                    List<String> schemaPath, List<String> viewPath, long lastModifiedAt) {
            super(null, viewSql, schemaPath, viewPath, modifiable);
            this.parsed = parsed;
            this.lastModifiedAt = lastModifiedAt;
        }

        @Override
        public TranslatableTable apply(List<Object> arguments) {
            return this.viewTable(parsed, viewSql, schemaPath, viewPath);
        }

        public long getLastModifiedAt() {
            return lastModifiedAt;
        }
    }

}
