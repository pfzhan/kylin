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
package io.kyligence.kap.query.util;

import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class SqlNodeExtractorTest extends NLocalFileMetadataTestCase {
    @Test
    public void testGetAllIdentifiers() throws Exception {
        String sql = "WITH a1 AS\n" + "  (SELECT * FROM t)\n" + "SELECT b.a1\n" + "FROM\n"
                + "  (WITH a2 AS (SELECT * FROM t) \n" + "    SELECT a2 FROM t2)\n" + "ORDER BY c_customer_id";
        SqlNodeExtractor sqlNodeExtractor = new SqlNodeExtractor();
        List<SqlIdentifier> identifiers = SqlNodeExtractor.getAllSqlIdentifier(sql);
        Assert.assertEquals(10, identifiers.size());
        val pos = SqlNodeExtractor.getIdentifierPos(sql);
        Assert.assertEquals(10, pos.size());
    }
}