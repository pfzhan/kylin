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
package io.kyligence.kap.tool;

import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.engine.ProjectSchemaFactory;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;

public class UpgradeCLI {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeCLI.class);
    private static final String SHOW_COLUMNS_FROM_SQL = "SELECT column_name FROM information_schema.columns WHERE table_name='%s' and column_name='%s'";
    private static final String ADD_COL_TO_TABLE_SQL = "alter table %s add %s %s";
    private static final String INSTANCE = "instance";

    public static void main(String[] args) throws Exception {
        logger.info("Start to upgrade...");
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        try (val auditLogStore = new JdbcAuditLogStore(kylinConfig)) {
            String auditLogTable = auditLogStore.getTable();
            String checkSql = String.format(SHOW_COLUMNS_FROM_SQL, auditLogTable, INSTANCE);
            String upgradeSql = String.format(ADD_COL_TO_TABLE_SQL, auditLogTable, INSTANCE, "varchar(100)");
            auditLogStore.checkAndUpgrade(checkSql, upgradeSql);
        }
        UpgradeCLI defaultDatabaseCLI = new UpgradeCLI();
        defaultDatabaseCLI.setDefaultDatabaseForAllProjects(kylinConfig);
        logger.info("Upgrade finished!");
    }

    void setDefaultDatabaseForAllProjects(KylinConfig kylinConfig) {
        logger.info("Start to set default database...");
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            setDefaultDatabase(project);
        }
        logger.info("Default database set finished!");
    }

    private void setDefaultDatabase(ProjectInstance project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            // for upgrade, set default database
            if (StringUtils.isEmpty(project.getDefaultDatabase())) {
                NProjectManager npr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                Collection<TableDesc> tables = npr.listDefinedTables(project.getName());
                HashMap<String, Integer> schemaCounts = DatabaseDesc.extractDatabaseOccurenceCounts(tables);
                String defaultDatabase = ProjectSchemaFactory.getDatabaseByMaxTables(schemaCounts);
                project.setDefaultDatabase(defaultDatabase.toUpperCase());
                npr.updateProject(project);
            }
            return 0;
        }, project.getName(), 1);
    }
}
