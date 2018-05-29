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

import io.kyligence.kap.smart.util.TableAliasGenerator.TableAliasDict;

public class TableAliasGeneratorTest {
    public static void main(String[] args) {
        String[] testTableNames = { "KYLIN_SALES",
                "TPCDS_BIN_PARTITIONED_ORC_2.CALL_CENTER",
                "TPCDS_BIN_PARTITIONED_ORC_2.CATALOG_PAGE",
                "TPCDS_BIN_PARTITIONED_ORC_2.CATALOG_RETURNS",
                "TPCDS_BIN_PARTITIONED_ORC_2.CATALOG_SALES",
                "TPCDS_BIN_PARTITIONED_ORC_2.CUSTOMER_ADDRESS",
                "TPCDS_BIN_PARTITIONED_ORC_2.CUSTOMER_DEMOGRAPHICS",
                "TPCDS_BIN_PARTITIONED_ORC_2.CUSTOMER",
                "TPCDS_BIN_PARTITIONED_ORC_2.DATE_DIM",
                "TPCDS_BIN_PARTITIONED_ORC_2.HOUSEHOLD_DEMOGRAPHICS",
                "TPCDS_BIN_PARTITIONED_ORC_2.INCOME_BAND",
                "TPCDS_BIN_PARTITIONED_ORC_2.INVENTORY",
                "TPCDS_BIN_PARTITIONED_ORC_2.ITEM",
                "TPCDS_BIN_PARTITIONED_ORC_2.PROMOTION",
                "TPCDS_BIN_PARTITIONED_ORC_2.REASON",
                "TPCDS_BIN_PARTITIONED_ORC_2.SHIP_MODE",
                "TPCDS_BIN_PARTITIONED_ORC_2.STORE_RETURNS",
                "TPCDS_BIN_PARTITIONED_ORC_2.STORE_SALES",
                "TPCDS_BIN_PARTITIONED_ORC_2.STORE",
                "TPCDS_BIN_PARTITIONED_ORC_2.TIME_DIM",
                "TPCDS_BIN_PARTITIONED_ORC_2.WAREHOUSE",
                "TPCDS_BIN_PARTITIONED_ORC_2.WEB_PAGE",
                "TPCDS_BIN_PARTITIONED_ORC_2.WEB_RETURNS",
                "TPCDS_BIN_PARTITIONED_ORC_2.WEB_SALES",
                "TPCDS_BIN_PARTITIONED_ORC_2.WEB_SITE"};
        
        TableAliasDict dict = TableAliasGenerator.generateNewDict(testTableNames);
        
        for (String table : testTableNames) {
            System.out.println(table + " => " + dict.getAlias(table));
        }

        System.out.println(dict.getHierachyAlias(new String[] { "TPCDS_BIN_PARTITIONED_ORC_2.STORE_SALES",
                "TPCDS_BIN_PARTITIONED_ORC_2.CUSTOMER_DEMOGRAPHICS", "TPCDS_BIN_PARTITIONED_ORC_2.CUSTOMER_ADDRESS" }));
    }
}
