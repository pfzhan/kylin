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
package io.kyligence.kap.tool.upgrade;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddInstanceColumnCLI {
    private static final String SHOW_COLUMNS_FROM_SQL = "SELECT column_name FROM information_schema.columns WHERE table_name='%s' and column_name='%s'";
    private static final String ADD_COL_TO_TABLE_SQL = "alter table %s add %s %s";
    private static final String INSTANCE = "instance";

    public static void main(String[] args) throws Exception {
        log.info("Start to add instance column to audit log...");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try (val auditLogStore = new JdbcAuditLogStore(kylinConfig)) {
            String auditLogTable = auditLogStore.getTable();
            String checkSql = String.format(SHOW_COLUMNS_FROM_SQL, auditLogTable, INSTANCE);
            String upgradeSql = String.format(ADD_COL_TO_TABLE_SQL, auditLogTable, INSTANCE, "varchar(100)");
            auditLogStore.checkAndUpgrade(checkSql, upgradeSql);
        }
        log.info("Add instance column finished!");
    }

}
