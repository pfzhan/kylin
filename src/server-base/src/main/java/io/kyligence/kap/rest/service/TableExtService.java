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

package io.kyligence.kap.rest.service;

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.shaded.influxdb.com.google.common.common.base.Function;
import io.kyligence.kap.shaded.influxdb.com.google.common.common.collect.Lists;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Component("tableExtService")
public class TableExtService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(TableExtService.class);


    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    /**
     * Load a group of  tables
     *
     * @return an array of table name sets:
     * [0] : tables that loaded successfully
     * [1] : tables that didn't load due to running sample job todo
     * [2] : tables that didn't load due to other error
     * @throws Exception if reading hive metadata error
     */
    public LoadTableResponse loadTables(String[] tables, String project, Integer sourceType) throws Exception {
        List<Pair<TableDesc, TableExtDesc>> extractTableMeta = tableService.extractTableMeta(tables, project,
                sourceType);
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        Set<String> loaded = Sets.newLinkedHashSet();
        Set<String> failed = Sets.newLinkedHashSet();
        for (Pair<TableDesc, TableExtDesc> pair : extractTableMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();
            String tableName = tableDesc.getIdentity();
            boolean ok;
            try {
                loadTable(tableDesc, extDesc, project);
                ok = true;
            } catch (Exception ex) {
                logger.error("Failed to load table '" + tableName + "'\"", ex);
                ok = false;
            }
            (ok ? loaded : failed).add(tableName);
        }
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        return loadTableResponse;
    }

    /**
     * Load given table to project
     *
     * @throws IOException on error
     */
    public void loadTable(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {

        String[] loaded = tableService.loadTableToProject(tableDesc, extDesc, project);
        // sanity check when loaded is empty or loaded table is not the table
        String tableName = tableDesc.getIdentity();
        if (loaded.length == 0 || !loaded[0].equals(tableName))
            throw new IllegalStateException();

    }

    public void removeJobIdFromTableExt(String jobId, String project) throws IOException {
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        for (TableDesc desc : tableMetadataManager.listAllTables()) {
            TableExtDesc extDesc = tableMetadataManager.getTableExt(desc);
            if (extDesc.getJodID() != null && jobId.equals(extDesc.getJodID())) {
                extDesc.setJodID(null);
                tableMetadataManager.saveTableExt(extDesc);
            }
        }

    }

    public LoadTableResponse loadTablesByDatabase(String project, final String[] databases, int datasourceType) throws Exception {
        LoadTableResponse loadTableByDatabaseResponse = new LoadTableResponse();
        for (final String database : databases) {
            List<String> tables = tableService.getSourceTableNames(project, database, datasourceType, "");
            List<String> identities = Lists.transform(tables, new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return database + "." + s;
                }
            });
            String[] tableToLoad = new String[identities.size()];
            LoadTableResponse loadTableResponse = loadTables(identities.toArray(tableToLoad), project, datasourceType);
            loadTableByDatabaseResponse.getLoaded().addAll(loadTableResponse.getLoaded());
            loadTableByDatabaseResponse.getFailed().addAll(loadTableResponse.getFailed());
        }
        return loadTableByDatabaseResponse;
    }
}
