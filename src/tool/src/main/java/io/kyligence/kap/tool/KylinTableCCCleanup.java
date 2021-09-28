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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;

public class KylinTableCCCleanup {

    private static final Logger logger = LoggerFactory.getLogger(KylinTableCCCleanup.class);

    private KylinConfig config;

    private boolean cleanup;

    private List<String> projects;

    public KylinTableCCCleanup(KylinConfig config, boolean cleanup, List<String> projects) {
        this.config = config;
        this.cleanup = cleanup;
        this.projects = projects;
    }

    public void scanAllTableCC() {
        for (String projectName : projects) {
            scanTableCCOfProject(projectName);
        }
    }

    private void scanTableCCOfProject(String projectName) {
        logger.info("check project {}", projectName);
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, projectName);
        Map<String, TableDesc> projectTableMap = tableMetadataManager.getAllTablesMap();
        Map<String, Set<ColumnDesc>> tableComputedColumns = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> tableDescEntry : projectTableMap.entrySet()) {
            String tableIdentity = tableDescEntry.getKey();
            TableDesc tableDesc = tableDescEntry.getValue();
            Set<ColumnDesc> computedColumnDescSet = getComputedColumnDescOfTable(tableDesc);
            if (!computedColumnDescSet.isEmpty()) {
                logger.info("check table: {}", tableIdentity);
                tableComputedColumns.put(tableIdentity, computedColumnDescSet);
                processTableHasCCInMetadata(projectName, tableDesc, computedColumnDescSet);
            }
        }

        if (cleanup) {
            logger.info("project {} cleanup finished successfully.", projectName);
        }
    }

    private void processTableHasCCInMetadata(String projectName, TableDesc tableDesc,
            Set<ColumnDesc> computedColumnDescSet) {
        Set<String> ccNames = Sets.newHashSet();
        for (ColumnDesc columnDesc : computedColumnDescSet) {
            ccNames.add(columnDesc.getName());
        }
        logger.info("project {} found computed columns in table metadata: {}", projectName, ccNames);
        if (cleanup) {
            cleanupCCInTableMetadata(projectName, tableDesc);
            logger.info("project {} table cleanup finished successfully.", projectName);
        }
    }

    private void cleanupCCInTableMetadata(String project, TableDesc tableDesc) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            TableDesc newTableDesc = filterCCBeforeSave(tableDesc);
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                    .updateTableDesc(newTableDesc);
            return 0;
        }, project);

    }

    private TableDesc filterCCBeforeSave(TableDesc srcTable) {
        TableDesc copy = new TableDesc(srcTable);
        Set<ColumnDesc> newCols = Sets.newHashSet();
        ColumnDesc[] cols = copy.getColumns();
        if (ArrayUtils.isEmpty(cols)) {
            return srcTable;
        }
        for (ColumnDesc col : cols) {
            if (!col.isComputedColumn()) {
                newCols.add(col);
            }
        }
        if (newCols.size() == cols.length) {
            return srcTable;
        } else {
            copy.setColumns(newCols.toArray(new ColumnDesc[0]));
            return copy;
        }
    }

    private Set<ColumnDesc> getComputedColumnDescOfTable(TableDesc tableDesc) {
        ColumnDesc[] columnDescArray = tableDesc.getColumns();
        if (null == columnDescArray) {
            return Sets.newHashSet();
        }
        Set<ColumnDesc> computedColumnDescSet = Sets.newHashSet();
        for (ColumnDesc columnDesc : columnDescArray) {
            if (columnDesc.isComputedColumn()) {
                computedColumnDescSet.add(columnDesc);
            }
        }
        return computedColumnDescSet;
    }
}
