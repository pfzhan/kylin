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

package io.kyligence.kap.metadata.acl;

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
