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

package org.apache.kylin.metadata.acl;

import org.apache.kylin.metadata.acl.AclTCR;
import org.junit.Assert;
import org.junit.Test;

public class AclTCRTest {
    @Test
    public void testIsAuthorized() {
        final String resourceName = "user1";
        final String tbl1 = "db1.tbl1";
        final String tbl2 = "db1.tbl2";
        final String col1 = "col1";
        final String col2 = "col2";
        final String fakeTbl = "db1.table1";
        final String fakeCol = "column1";
        AclTCR aclTCR = new AclTCR();
        aclTCR.init(resourceName);
        AclTCR.Table table = new AclTCR.Table();
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        AclTCR.Column column = new AclTCR.Column();
        column.add(col1);
        column.add(col2);
        AclTCR.Row row = new AclTCR.Row();
        AclTCR.RealRow realRow = new AclTCR.RealRow();
        realRow.add("row1");
        realRow.add("row2");
        row.put(col1, realRow);
        row.put(col2, null);
        columnRow.setRow(row);
        columnRow.setColumn(column);
        table.put(tbl1, columnRow);
        table.put(tbl2, null);
        aclTCR.setTable(table);

        Assert.assertTrue(aclTCR.isAuthorized(tbl1));
        Assert.assertTrue(aclTCR.isAuthorized(tbl2));
        Assert.assertTrue(new AclTCR().isAuthorized(tbl1));
        Assert.assertTrue(aclTCR.isAuthorized(tbl1, col1));
        Assert.assertTrue(aclTCR.isAuthorized(tbl2, col1));
        Assert.assertTrue(new AclTCR().isAuthorized(tbl1, col1));

        Assert.assertFalse(aclTCR.isAuthorized(fakeTbl));
        Assert.assertFalse(aclTCR.isAuthorized(tbl1, fakeCol));
        Assert.assertFalse(aclTCR.isAuthorized(fakeTbl, fakeCol));

        Assert.assertEquals(aclTCR.getTable(), table);
        Assert.assertEquals(columnRow.getColumn(), column);
        Assert.assertEquals(columnRow.getRow(), row);
    }
}
