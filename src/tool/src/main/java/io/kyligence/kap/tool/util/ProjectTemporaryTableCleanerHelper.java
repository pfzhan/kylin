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

package io.kyligence.kap.tool.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.hive.HiveCmdBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.kyligence.kap.engine.spark.utils.HiveTransactionTableHelper.generateDropTableStatement;
import static io.kyligence.kap.engine.spark.utils.HiveTransactionTableHelper.generateHiveInitStatements;

@Slf4j
public class ProjectTemporaryTableCleanerHelper {
    private final String TRANSACTIONAL_TABLE_NAME_SUFFIX = "_hive_tx_intermediate";

    public Map<String, List<String>> collectDropDBTemporaryTableNameMap(ISourceMetadataExplorer explr,
                                                                        List<StorageCleaner.FileTreeNode> jobTmps,
                                                                        Set<String> discardJobs) throws Exception {
        Map<String, List<String>> dropDbTableNameMap = Maps.newConcurrentMap();
        explr.listDatabases().forEach(dbName -> {
            try {
                explr.listTables(dbName).stream().filter(tableName -> tableName.contains(TRANSACTIONAL_TABLE_NAME_SUFFIX))
                        .filter(tableName -> isMatchesTemporaryTables(jobTmps, discardJobs, tableName))
                        .forEach(tableName -> putTableNameToDropDbTableNameMap(dropDbTableNameMap, dbName, tableName));
            } catch (Exception exception) {
                log.error("Failed to get the table name in the data, database: " + dbName, exception);
            }
        });
        return dropDbTableNameMap;
    }

    public boolean isMatchesTemporaryTables(List<StorageCleaner.FileTreeNode> jobTemps, Set<String> discardJobs, String tableName) {
        val suffixId = tableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX).length == 2
                ? tableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX)[1] : tableName;
        val discardJobMatch = discardJobs.stream().anyMatch(jobId -> jobId.endsWith(suffixId));
        val jobTempMatch = jobTemps.stream().anyMatch(node -> node.getRelativePath().endsWith(suffixId));
        return discardJobMatch || jobTempMatch;
    }

    public void putTableNameToDropDbTableNameMap(Map<String, List<String>> dropTableDbNameMap,
                                                 String dbName, String tableName) {
        List<String> tableNameList = dropTableDbNameMap.containsKey(dbName)
                ? dropTableDbNameMap.get(dbName) : Lists.newArrayList();
        tableNameList.add(tableName);
        dropTableDbNameMap.put(dbName, tableNameList);
    }

    public boolean isNeedClean(boolean isEmptyJobTmp, boolean isEmptyDiscardJob) {
        return !isEmptyJobTmp || !isEmptyDiscardJob;
    }

    public String collectDropDBTemporaryTableCmd(KylinConfig kylinConfig, ISourceMetadataExplorer explr,
                                                 List<StorageCleaner.FileTreeNode> jobTemps,
                                                 Set<String> discardJobs) throws Exception {
        Map<String, List<String>> dropDbTableNameMap = collectDropDBTemporaryTableNameMap(explr, jobTemps, discardJobs);
        log.info("List of tables that meet the conditions:{}", dropDbTableNameMap);
        if(!dropDbTableNameMap.isEmpty()) {
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(kylinConfig);
            dropDbTableNameMap.forEach((dbName, tableNameList) ->
                    tableNameList.forEach(tableName ->
                            hiveCmdBuilder.addStatement(generateHiveInitStatements(dbName) + generateDropTableStatement(tableName))
                    )
            );
            return hiveCmdBuilder.toString();
        }
        return "";
    }
}
