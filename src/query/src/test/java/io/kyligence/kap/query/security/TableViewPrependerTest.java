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

package io.kyligence.kap.query.security;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableViewPrependerTest {

    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources";

    private static final String LINEITEM = "tpch.lineitem";
    private static final String PART = "tpch.part";
    private static final String SUPPLIER = "tpch.supplier";
    private static final String PART_SUPP = "tpch.partsupp";
    private static final String NATION = "tpch.nation";
    private static final String REGION = "tpch.region";
    private static final String ORDERS = "tpch.orders";
    private static final String CUSTOMER = "tpch.customer";

    @Test
    public void testTableIdentifierVisitor() throws IOException {
        val tableViewPrepender = new TableViewPrepender();
        val sqls = getSqls("sql_tpch");
        val expectedTables = getExpectedTableIdentifiers();
        val expectedTableIdentifiers = expectedTables.getFirst();
        val expectedWithClauses = expectedTables.getSecond();
        for (int i = 1; i <= sqls.size(); i++) {
            val sql = sqls.get(i-1);
            val pair = tableViewPrepender.getTableIdentifiers(sql);
            val allTableIdentifiers = pair.getFirst();
            val withClauses = pair.getSecond();
            assertTableIdentifiers(expectedTableIdentifiers.get(i), expectedWithClauses.get(i), allTableIdentifiers, withClauses);
        }
    }

    private Pair<Map<Integer, List<String>>, Map<Integer, Map<String, List<String>>>> getExpectedTableIdentifiers() {
        val tableIdentifierMap = Maps.<Integer, List<String>>newHashMap();
        val withClauseMap = Maps.<Integer, Map<String, List<String>>>newHashMap();
        tableIdentifierMap.put(1, Lists.newArrayList(LINEITEM));
        tableIdentifierMap.put(2, Lists.newArrayList(PART, SUPPLIER, PART_SUPP, NATION, REGION, "q2_min_ps_supplycost"));
        tableIdentifierMap.put(3, Lists.newArrayList(LINEITEM, ORDERS, CUSTOMER));
        tableIdentifierMap.put(4, Lists.newArrayList(ORDERS, LINEITEM));
        tableIdentifierMap.put(5, Lists.newArrayList(LINEITEM, ORDERS, CUSTOMER, NATION, SUPPLIER, NATION, REGION));
        tableIdentifierMap.put(6, Lists.newArrayList(LINEITEM));
        tableIdentifierMap.put(7, Lists.newArrayList(LINEITEM, SUPPLIER, ORDERS, CUSTOMER, NATION, NATION));
        tableIdentifierMap.put(8, Lists.newArrayList(LINEITEM, PART, SUPPLIER, ORDERS, CUSTOMER, NATION, NATION, REGION));
        tableIdentifierMap.put(9, Lists.newArrayList(LINEITEM, PART, SUPPLIER, PART_SUPP, ORDERS, NATION));
        tableIdentifierMap.put(10, Lists.newArrayList(LINEITEM, ORDERS, CUSTOMER, NATION));
        tableIdentifierMap.put(11, Lists.newArrayList("q11_part_tmp_cached", "q11_sum_tmp_cached"));
        tableIdentifierMap.put(12, Lists.newArrayList(LINEITEM, ORDERS));
        tableIdentifierMap.put(13, Lists.newArrayList(CUSTOMER, ORDERS));
        tableIdentifierMap.put(14, Lists.newArrayList(LINEITEM, PART));
        tableIdentifierMap.put(15, Lists.newArrayList(SUPPLIER, "revenue_cached", "max_revenue_cached"));
        tableIdentifierMap.put(16, Lists.newArrayList(PART_SUPP, PART, SUPPLIER));
        tableIdentifierMap.put(17, Lists.newArrayList("q17_avg", "q17_price"));
        tableIdentifierMap.put(18, Lists.newArrayList(CUSTOMER, ORDERS, "q18_tmp_cached", LINEITEM));
        tableIdentifierMap.put(19, Lists.newArrayList(LINEITEM, PART));
        tableIdentifierMap.put(20, Lists.newArrayList(SUPPLIER, "tmp5"));
        tableIdentifierMap.put(21, Lists.newArrayList(LINEITEM, ORDERS, SUPPLIER, NATION, LINEITEM, ORDERS, LINEITEM, ORDERS));
        tableIdentifierMap.put(22, Lists.newArrayList("q22_customer_tmp1_cached", "q22_orders_tmp_cached", "q22_customer_tmp_cached"));
        tableIdentifierMap.put(23, Lists.newArrayList(LINEITEM, PART));


        withClauseMap.put(2, new HashMap<String, List<String>>() {{
            put("q2_min_ps_supplycost", Lists.newArrayList(PART, PART_SUPP, SUPPLIER, NATION, REGION));
        }});
        withClauseMap.put(11, new HashMap<String, List<String>>() {{
            put("q11_part_tmp_cached", Lists.newArrayList(PART_SUPP, SUPPLIER, NATION));
            put("q11_sum_tmp_cached", Lists.newArrayList("q11_part_tmp_cached"));
        }});
        withClauseMap.put(15, new HashMap<String, List<String>>() {{
            put("revenue_cached", Lists.newArrayList(LINEITEM));
            put("max_revenue_cached", Lists.newArrayList("revenue_cached"));
        }});
        withClauseMap.put(17, new HashMap<String, List<String>>() {{
            put("q17_part", Lists.newArrayList(PART));
            put("q17_avg", Lists.newArrayList(LINEITEM, "q17_part"));
            put("q17_price", Lists.newArrayList(LINEITEM, "q17_part"));
        }});
        withClauseMap.put(18, new HashMap<String, List<String>>() {{
            put("q18_tmp_cached", Lists.newArrayList(LINEITEM));
        }});
        withClauseMap.put(20, new HashMap<String, List<String>>() {{
            put("tmp1", Lists.newArrayList(PART));
            put("tmp2", Lists.newArrayList(SUPPLIER, NATION));
            put("tmp3", Lists.newArrayList(LINEITEM, "tmp2"));
            put("tmp4", Lists.newArrayList(PART_SUPP, "tmp1"));
            put("tmp5", Lists.newArrayList("tmp4", "tmp3"));
        }});
        withClauseMap.put(22, new HashMap<String, List<String>>() {{
            put("q22_customer_tmp_cached", Lists.newArrayList(CUSTOMER));
            put("q22_customer_tmp1_cached", Lists.newArrayList("q22_customer_tmp_cached"));
            put("q22_orders_tmp_cached", Lists.newArrayList(ORDERS));
        }});

        return Pair.newPair(tableIdentifierMap, withClauseMap);
    }

    private void assertTableIdentifiers(List<String> expectedTableIdentifiers, Map<String, List<String>> expectedWithClauses,
                                        List<SqlIdentifier> actualTableIdentifiers, Map<String, List<SqlIdentifier>> actualWithClauses) {
        Assert.assertEquals(expectedTableIdentifiers.size(), actualTableIdentifiers.size());
        int size = expectedTableIdentifiers.size();
        for (int i = 0; i < size; i++) {
            val expectedTableIdentifier = expectedTableIdentifiers.get(i);
            val actualTableIdentifier = actualTableIdentifiers.get(i);
            Assert.assertEquals(expectedTableIdentifier.toUpperCase(), actualTableIdentifier.toString());
        }

        if (expectedWithClauses == null) {
            Assert.assertTrue(actualWithClauses == null || actualWithClauses.isEmpty());
            return;
        }
        Assert.assertEquals(expectedWithClauses.size(), actualWithClauses.size());
        for (Map.Entry<String, List<String>> entry : expectedWithClauses.entrySet()) {
            val withAlias = entry.getKey().toUpperCase();
            val expectedTables = entry.getValue();

            val actualTables = actualWithClauses.get(withAlias);
            Assert.assertNotNull(actualTables);
            int tableSize = expectedTables.size();
            for (int i = 0; i < tableSize; i++) {
                val expectedTable = expectedTables.get(i);
                val actualTable = actualTables.get(i);
                Assert.assertEquals(expectedTable.toUpperCase(), actualTable.toString());
            }
        }
    }

    private List<String> getSqls(String subFolder) throws IOException {
        String folder = IT_SQL_KAP_DIR + File.separator + subFolder;
        val file = new File(folder);
        return retrieveITSqls(file);
    }

    private static List<String> retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null && file.exists() && file.listFiles() != null) {
            sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
        }
        List<String> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, (o1, o2) -> {
            final String idxStr1 = o1.getName().replaceAll("\\D", "");
            final String idxStr2 = o2.getName().replaceAll("\\D", "");
            if (idxStr1.isEmpty() || idxStr2.isEmpty()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
            return Integer.parseInt(idxStr1) - Integer.parseInt(idxStr2);
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex)
                    : sqlStatement;
            ret.add(sql + '\n');
        }
        return ret;
    }
}
