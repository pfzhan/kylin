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

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import java.util.Collections;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.query.QueryExtension;
import io.kyligence.kap.query.util.QueryContextCutter;
import lombok.val;

@MetadataInfo(project = "default")
class SelectRealizationTest {

    @BeforeEach
    public void setUp() throws Exception {
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    @Test
    void testDerivedFromSameContext() throws SqlParseException {
        val kylinConfig = getTestConfig();
        val config = KECalciteConfig.fromKapConfig(kylinConfig);
        val schemaFactory = new ProjectSchemaFactory("default", kylinConfig);
        val rootSchema = schemaFactory.createProjectRootSchema();
        String defaultSchemaName = schemaFactory.getDefaultSchema();
        val catalogReader = createCatalogReader(config, rootSchema, defaultSchemaName);
        val planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(config);
        val sqlConverter = SQLConverter.createConverter(config, planner, catalogReader);
        val queryOptimizer = new QueryOptimizer(planner);
        RelRoot relRoot = sqlConverter
                .convertSqlToRelNode("SELECT count(1)\n" + "FROM \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                        + "JOIN (SELECT MAX(\"LINEORDER\".\"LO_ORDERDATE\") AS \"X_measure__0\"\n"
                        + "FROM \"SSB\".\"LINEORDER\" \"LINEORDER\"\n" + "GROUP BY 1.1000000000000001 ) \"t0\"\n"
                        + "ON LINEORDER.LO_ORDERDATE = t0.X_measure__0\n" + "GROUP BY 1.1000000000000001");
        RelNode node = queryOptimizer.optimize(relRoot).rel;
        val olapContexts = QueryContextCutter.selectRealization(node, BackdoorToggles.getIsQueryFromAutoModeling());
        Assertions.assertNotNull(olapContexts);
        Assertions.assertFalse(olapContexts.isEmpty());
    }

    private Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig,
            CalciteSchema rootSchema, String defaultSchemaName) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(rootSchema, Collections.singletonList(defaultSchemaName), javaTypeFactory,
                connectionConfig);
    }
}
