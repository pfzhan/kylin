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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job;

import java.util.List;
import java.util.UUID;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableBiMap;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;

public class JoinedFlatTableTest {

    private NDataModel dataModel = new NDataModel();

    @Before
    public void setUp() {
        dataModel.setUuid(UUID.randomUUID().toString());

        TableDesc lineOrderTableDesc = new TableDesc();
        lineOrderTableDesc.setUuid("665a66d6-08d6-42b8-9be8-d9a0456ee250");
        lineOrderTableDesc.setName("LINEORDER");
        lineOrderTableDesc.setDatabase("SSB");
        lineOrderTableDesc.setSourceType(9);
        lineOrderTableDesc.setTableType("MANAGED");

        ColumnDesc loSuppkeyColDesc = new ColumnDesc();
        loSuppkeyColDesc.setId("1");
        loSuppkeyColDesc.setName("LO_SUPPKEY");
        loSuppkeyColDesc.setDatatype("integer");
        loSuppkeyColDesc.setComment("null");
        loSuppkeyColDesc.setTable(lineOrderTableDesc);

        ColumnDesc loTaxColDesc = new ColumnDesc();
        loTaxColDesc.setId("2");
        loTaxColDesc.setName("LO_TAX");
        loTaxColDesc.setDatatype("bigint");
        loTaxColDesc.setComment("null");
        loTaxColDesc.setTable(lineOrderTableDesc);

        ColumnDesc loRevenueColDesc = new ColumnDesc();
        loRevenueColDesc.setId("3");
        loRevenueColDesc.setName("LO_REVENUE");
        loRevenueColDesc.setDatatype("bigint");
        loRevenueColDesc.setComment("null");
        loRevenueColDesc.setTable(lineOrderTableDesc);

        ColumnDesc cc = new ColumnDesc("4", "PROFIT", "bigint", null, null, null,
                "LINEORDER.LO_REVENUE-LINEORDER.LO_TAX");
        cc.setTable(lineOrderTableDesc);

        lineOrderTableDesc.setColumns(new ColumnDesc[] { loSuppkeyColDesc, loTaxColDesc, loRevenueColDesc, cc });
        TableRef lineOrderTableRef = new TableRef(dataModel, "LINEORDER", lineOrderTableDesc, false);

        TableDesc supplierTableDesc = new TableDesc();
        supplierTableDesc.setUuid("719e9bf4-82de-40ec-9454-ab43ff94eef4");
        supplierTableDesc.setName("SUPPLIER");
        supplierTableDesc.setDatabase("SSB");
        supplierTableDesc.setSourceType(9);
        supplierTableDesc.setTableType("MANAGED");

        ColumnDesc sSuppkey = new ColumnDesc();
        sSuppkey.setId("1");
        sSuppkey.setName("S_SUPPKEY");
        sSuppkey.setDatatype("integer");
        sSuppkey.setComment("null");
        sSuppkey.setTable(supplierTableDesc);

        ColumnDesc sCity = new ColumnDesc();
        sCity.setId("2");
        sCity.setName("S_CITY");
        sCity.setDatatype("varchar(4096)");
        sCity.setComment("null");
        sCity.setTable(supplierTableDesc);

        supplierTableDesc.setColumns(new ColumnDesc[] { sSuppkey, sCity });
        TableRef supplierTableRef = new TableRef(dataModel, "SUPPLIER", supplierTableDesc, false);

        ImmutableBiMap.Builder<Integer, TblColRef> effectiveCols = ImmutableBiMap.builder();
        effectiveCols.put(1, lineOrderTableRef.getColumn("LO_SUPPKEY"));
        effectiveCols.put(2, lineOrderTableRef.getColumn("LO_REVENUE"));
        effectiveCols.put(3, lineOrderTableRef.getColumn("LO_TAX"));
        effectiveCols.put(4, lineOrderTableRef.getColumn("PROFIT"));
        effectiveCols.put(5, supplierTableRef.getColumn("S_SUPPKEY"));
        effectiveCols.put(6, supplierTableRef.getColumn("S_CITY"));
        dataModel.setEffectiveCols(effectiveCols.build());

        dataModel.setRootFactTableName("SSB.LINEORDER");
        dataModel.setRootFactTableRef(lineOrderTableRef);

        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType("LEFT");
        joinDesc.setPrimaryKey(new String[] { "LINEORDER.LO_SUPPKEY" });
        joinDesc.setForeignKey(new String[] { "SUPPLIER.S_SUPPKEY" });
        joinDesc.setPrimaryTableRef(lineOrderTableRef);
        joinDesc.setPrimaryKeyColumns(new TblColRef[] { new TblColRef(lineOrderTableRef, loSuppkeyColDesc) });
        joinDesc.setForeignKeyColumns(new TblColRef[] { new TblColRef(supplierTableRef, sSuppkey) });
        JoinTableDesc supplierJoinTableDesc = new JoinTableDesc();
        supplierJoinTableDesc.setTable("SSB.SUPPLIER");
        supplierJoinTableDesc.setAlias("SUPPLIER");
        supplierJoinTableDesc.setKind(NDataModel.TableKind.LOOKUP);
        supplierJoinTableDesc.setTableRef(supplierTableRef);
        supplierJoinTableDesc.setJoin(joinDesc);

        dataModel.setJoinTables(Lists.newArrayList(supplierJoinTableDesc));
        dataModel.setFilterCondition("SUPPLIER.S_CITY != 'beijing'");
        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setExpression("LINEORDER.LO_REVENUE-LINEORDER.LO_TAX");
        cc1.setInnerExpression("LINEORDER.LO_REVENUE-LINEORDER.LO_TAX");
        cc1.setColumnName("PROFIT");
        cc1.setDatatype("bigint");
        dataModel.setComputedColumnDescs(Lists.newArrayList(cc1));
    }

    @Test
    public void testQuoteIdentifierInSqlExpr() {
        String cc = JoinedFlatTable.quoteIdentifierInSqlExpr(dataModel, "LINEORDER.LO_REVENUE-LINEORDER.LO_TAX", "`");
        Assert.assertEquals("`LINEORDER`.`LO_REVENUE`-`LINEORDER`.`LO_TAX`", cc);

        String where1 = JoinedFlatTable.quoteIdentifierInSqlExpr(dataModel, "LINEORDER.LO_REVENUE-LINEORDER.LO_TAX>0",
                "`");
        Assert.assertEquals("`LINEORDER`.`LO_REVENUE`-`LINEORDER`.`LO_TAX`>0", where1);

        String where2 = JoinedFlatTable.quoteIdentifierInSqlExpr(dataModel,
                "LINEORDER.LO_REVENUE>100 AND LINEORDER.LO_TAX>0", "`");
        Assert.assertEquals("`LINEORDER`.`LO_REVENUE`>100 AND `LINEORDER`.`LO_TAX`>0", where2);
    }

    @Test
    public void testGenerateSelectDataStatement() {
        String flatTableSql = JoinedFlatTable.generateSelectDataStatement(dataModel, false);
        Assert.assertTrue(flatTableSql.contains("\"SUPPLIER\".\"S_CITY\" != 'beijing'"));
        Assert.assertTrue(flatTableSql.contains("SELECT "));
        Assert.assertTrue(flatTableSql.contains("FROM "));
        Assert.assertTrue(flatTableSql.contains("WHERE "));
        Assert.assertTrue(flatTableSql.contains("\"LINEORDER\".\"LO_SUPPKEY\" as \"LINEORDER_LO_SUPPKEY\","));
        Assert.assertTrue(flatTableSql.contains("\"LINEORDER\".\"LO_REVENUE\" as \"LINEORDER_LO_REVENUE\","));
        Assert.assertTrue(flatTableSql.contains("\"LINEORDER\".\"LO_TAX\" as \"LINEORDER_LO_TAX\","));
        Assert.assertTrue(flatTableSql
                .contains("\"LINEORDER\".\"LO_REVENUE\"-\"LINEORDER\".\"LO_TAX\" as \"LINEORDER_PROFIT\","));
        Assert.assertTrue(flatTableSql.contains("\"SUPPLIER\".\"S_SUPPKEY\" as \"SUPPLIER_S_SUPPKEY\","));
        Assert.assertTrue(flatTableSql.contains("\"SUPPLIER\".\"S_CITY\" as \"SUPPLIER_S_CITY\""));
        Assert.assertTrue(flatTableSql.contains("\"SSB\".\"LINEORDER\" as \"LINEORDER\""));
        Assert.assertTrue(flatTableSql.contains("LEFT JOIN \"SSB\".\"SUPPLIER\" as \"SUPPLIER\""));
        Assert.assertTrue(flatTableSql.contains("ON \"SUPPLIER\".\"S_SUPPKEY\"=\"LINEORDER\".\"LO_SUPPKEY\""));
        Assert.assertTrue(flatTableSql.contains("\"SUPPLIER\".\"S_CITY\" != 'beijing'"));

        NonEquiJoinCondition nonEquiJoinCondition = new NonEquiJoinCondition();
        nonEquiJoinCondition.setExpr("SUPPLIER.S_SUPPKEY <> LINEORDER.LO_SUPPKEY AND LINEORDER.LO_SUPPKEY > 10");
        dataModel.getJoinTables().get(0).getJoin().setNonEquiJoinCondition(nonEquiJoinCondition);
        String nonEquiJoinConditionSql = JoinedFlatTable.generateSelectDataStatement(dataModel, false);
        Assert.assertTrue(nonEquiJoinConditionSql.contains(
                "ON \"SUPPLIER\".\"S_SUPPKEY\" <> \"LINEORDER\".\"LO_SUPPKEY\" AND \"LINEORDER\".\"LO_SUPPKEY\" > 10"));
        dataModel.getJoinTables().get(0).getJoin().setNonEquiJoinCondition(null);
    }

    @Test
    public void testQuoteIdentifier() {
        List<String> tablePatterns = JoinedFlatTable.getTableNameOrAliasPatterns("KYLIN_SALES");
        String exprTable = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String expectedExprTable = "`KYLIN_SALES`.PRICE * `KYLIN_SALES`.COUNT";
        String quotedExprTable = JoinedFlatTable.quoteIdentifier(exprTable, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExprTable, quotedExprTable);

        exprTable = "`KYLIN_SALES`.PRICE * KYLIN_SALES.COUNT";
        expectedExprTable = "`KYLIN_SALES`.PRICE * `KYLIN_SALES`.COUNT";
        quotedExprTable = JoinedFlatTable.quoteIdentifier(exprTable, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExprTable, quotedExprTable);

        exprTable = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        expectedExprTable = "`KYLIN_SALES`.PRICE AS KYLIN_SALES_PRICE * `KYLIN_SALES`.COUNT AS KYLIN_SALES_COUNT";
        quotedExprTable = JoinedFlatTable.quoteIdentifier(exprTable, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExprTable, quotedExprTable);

        exprTable = "(KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE > 1 and KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT > 50)";
        expectedExprTable = "(`KYLIN_SALES`.PRICE AS KYLIN_SALES_PRICE > 1 and `KYLIN_SALES`.COUNT AS KYLIN_SALES_COUNT > 50)";
        quotedExprTable = JoinedFlatTable.quoteIdentifier(exprTable, "`", "KYLIN_SALES", tablePatterns);
        Assert.assertEquals(expectedExprTable, quotedExprTable);

        List<String> columnPatterns = JoinedFlatTable.getColumnNameOrAliasPatterns("PRICE");
        String expr = "KYLIN_SALES.PRICE * KYLIN_SALES.COUNT";
        String expectedExpr = "KYLIN_SALES.`PRICE` * KYLIN_SALES.COUNT";
        String quotedExpr = JoinedFlatTable.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE/KYLIN_SALES.COUNT";
        expectedExpr = "KYLIN_SALES.`PRICE`/KYLIN_SALES.COUNT";
        quotedExpr = JoinedFlatTable.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "KYLIN_SALES.PRICE AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        expectedExpr = "KYLIN_SALES.`PRICE` AS KYLIN_SALES_PRICE * KYLIN_SALES.COUNT AS KYLIN_SALES_COUNT";
        quotedExpr = JoinedFlatTable.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "(PRICE > 1 AND COUNT > 50)";
        expectedExpr = "(`PRICE` > 1 AND COUNT > 50)";
        quotedExpr = JoinedFlatTable.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);

        expr = "PRICE>1 and `PRICE` < 15";
        expectedExpr = "`PRICE`>1 and `PRICE` < 15";
        quotedExpr = JoinedFlatTable.quoteIdentifier(expr, "`", "PRICE", columnPatterns);
        Assert.assertEquals(expectedExpr, quotedExpr);
    }
}
