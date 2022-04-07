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

package io.kyligence.kap.smart.util;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.AutoTestOnLearnKylinData;
import io.kyligence.kap.smart.util.TableAliasGenerator.TableAliasDict;

public class TableAliasGeneratorTest extends AutoTestOnLearnKylinData {

    @Test
    public void testJoinHierarchy() {
        String[] testTableNames = { "TABLEA", "TABLEB", "TABLEC", "TABLED" };
        TableAliasDict dict = TableAliasGenerator.generateNewDict(testTableNames);

        TableRef tableARef = mockTableRef("TABLEA", "COLA");
        TableRef tableBRef = mockTableRef("TABLEB", "COLB");
        TableRef tableCRef = mockTableRef("TABLEC", "COL");
        TableRef tableDRef = mockTableRef("TABLED", "COL");
        String joinHierarchy = dict.getHierarchyAliasFromJoins(new JoinDesc[] { //
                mockJoinDesc(tableBRef, tableARef, "COLB", "COLA"), //
                mockJoinDesc(tableCRef, tableBRef, "COL", "COLB"), //
                mockJoinDesc(tableDRef, tableBRef, "COL", "COLB") });
        Assert.assertEquals(
                "T0_KEY_[COLA]__TO__T1_KEY_[COLB]_KEY_[COLB]__TO__T2_KEY_[COL]_KEY_[COLB]__TO__T3_KEY_[COL]",
                joinHierarchy);
    }

    @Test
    public void testTableAliasGenerator() {
        String[] testTableNames = { "database1.table1", "database1.table3", "database1.table2", //
                "database2.table1", "database2.table4", "database2.table3", "database2.table2" };
        TableAliasDict dict = TableAliasGenerator.generateNewDict(testTableNames);
        Assert.assertEquals("D0_T0", dict.getAlias("database1.table1"));
        Assert.assertEquals("D0_T1", dict.getAlias("database1.table2"));
        Assert.assertEquals("database2.table4", dict.getTableName("D1_T3"));
        Assert.assertEquals("", dict.getHierarchyAliasFromJoins(null));
    }

    private TableRef mockTableRef(String tableName, String col) {
        TableDesc tableDesc = TableDesc.mockup(tableName);
        tableDesc.setColumns(new ColumnDesc[] { ColumnDesc.mockup(tableDesc, 0, col, "string") });
        return new TableRef(new NDataModel(), tableName, tableDesc, false);
    }

    private JoinDesc mockJoinDesc(TableRef pTable, TableRef fTable, String pk, String fk) {
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setPrimaryKey(new String[] { pk });
        joinDesc.setForeignKey(new String[] { fk });
        joinDesc.setPrimaryKeyColumns(new TblColRef[] { pTable.getColumn(pk) });
        joinDesc.setForeignKeyColumns(new TblColRef[] { fTable.getColumn(fk) });
        joinDesc.setPrimaryTableRef(pTable);
        joinDesc.setForeignTableRef(fTable);
        joinDesc.setType("LEFT");
        return joinDesc;
    }
}
