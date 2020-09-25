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

package io.kyligence.kap.query.calcite;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Frameworks;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.junit.Assert.fail;

public class ValidatorTest {
    SqlValidator validator;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws SqlParseException {
        RelDataTypeFactory factory = new SqlTypeFactoryImpl(new KylinRelDataTypeSystem());
        SchemaPlus rootScheme = Frameworks.createRootSchema(true);
        rootScheme.add("PEOPLE", new AbstractTable() { //note: add a table
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                builder.add("ID", factory.createSqlType(SqlTypeName.INTEGER));
                builder.add("NAME", factory.createSqlType(SqlTypeName.VARCHAR));
                builder.add("AGE", factory.createSqlType(SqlTypeName.INTEGER));
                builder.add("BIRTHDAY", factory.createSqlType(SqlTypeName.DATE));
                return builder.build();
            }
        });
        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(CalciteSchema.from(rootScheme),
                CalciteSchema.from(rootScheme).path(null), factory, new CalciteConnectionConfigImpl(new Properties()));
        validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader, factory);
    }

    @Test
    public void testImplicitTypeCast() {
        try {
            validator.validate(parse("select sum(AGE + NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE - NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE * NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE / NAME) from PEOPLE "));
            validator.validate(parse("select * from PEOPLE where BIRTHDAY = cast('2013-01-01 00:00:01' as timestamp)"));
        } catch (Exception e) {
            fail("Should not have thrown any exception");
        }
    }

    private SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        return parser.parseQuery();
    }

}
