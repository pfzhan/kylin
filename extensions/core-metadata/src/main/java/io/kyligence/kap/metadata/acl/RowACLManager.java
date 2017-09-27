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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class RowACLManager {

    private static final Logger logger = LoggerFactory.getLogger(RowACLManager.class);

    private static final Serializer<RowACL> ROW_ACL_SERIALIZER = new JsonSerializer<>(RowACL.class);
    private static final String DIR_PREFIX = "/row_acl/";

    // static cached instances
    private static final ConcurrentMap<KylinConfig, RowACLManager> CACHE = new ConcurrentHashMap<>();

    public static RowACLManager getInstance(KylinConfig config) {
        RowACLManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RowACLManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RowACLManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeDescManager from " + config, e);
            }
        }
    }

    private static void clearCache() {
        CACHE.clear();
    }

    public static void clearCache(KylinConfig kylinConfig) {
        if (kylinConfig != null)
            CACHE.remove(kylinConfig);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> RowACL
    private CaseInsensitiveStringCache<RowACL> rowACLMap;

    public RowACLManager(KylinConfig config) throws IOException {
        logger.info("Initializing RowACLManager with config " + config);
        this.config = config;
        this.rowACLMap = new CaseInsensitiveStringCache<>(config, "row_acl");
        loadAllRowACL();
        Broadcaster.getInstance(config).registerListener(new RowACLSyncListener(), "row_acl");
    }

    private class RowACLSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey) throws IOException {
            reloadRowACL(cacheKey);
            broadcaster.notifyProjectACLUpdate(cacheKey);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public RowACL getRowACLByCache(String project){
        RowACL tableACL = rowACLMap.get(project);
        if (tableACL == null) {
            return new RowACL();
        }
        return tableACL;
    }

    private void loadAllRowACL() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively("/row_acl", "");
        final int prefixLen = DIR_PREFIX.length();
        for (String path : paths) {
            String project = path.substring(prefixLen, path.length());
            reloadRowACL(project);
        }
        logger.info("Loading row ACL from folder " + store.getReadableResourcePath("/row_acl"));
    }

    private void reloadRowACL(String project) throws IOException {
        RowACL tableACLRecord = getRowACL(project);
        rowACLMap.putLocal(project, tableACLRecord);
    }

    private RowACL getRowACL(String project) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACLRecord = getStore().getResource(path, RowACL.class, ROW_ACL_SERIALIZER);
        if (rowACLRecord == null || rowACLRecord.getTableRowCondsWithUser() == null) {
            return new RowACL();
        }
        return rowACLRecord;
    }

    //user1:{col1:[a, b, c], col2:[d]}
    public Map<String, Map<String, List<RowACL.Cond>>> getRowCondListByTable(String project, String table) throws IOException {
        Map<String, RowACL.TableRowCondList> tableRowCondsWithUser = getRowACL(project).getTableRowCondsWithUser();
        Map<String, Map<String, List<RowACL.Cond>>> results = new HashMap<>();
        for (String user : tableRowCondsWithUser.keySet()) {
            RowACL.TableRowCondList tableRowCondList = tableRowCondsWithUser.get(user);
            RowACL.RowCondList rowCondListByTable = tableRowCondList.getRowCondListByTable(table);
            if (!rowCondListByTable.isEmpty()) {
                results.put(user, rowCondListByTable.getCondsWithColumn());
            }
        }
        return results;
    }

    public Map<String, String> getQueryUsedCondsByUser(String project, String username) {
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, RowACL.TableRowCondList> tableRowCondsWithUser = getRowACLByCache(project).getTableRowCondsWithUser();
        if (tableRowCondsWithUser == null || tableRowCondsWithUser.isEmpty()) {
            return result;
        }

        RowACL.TableRowCondList tableRowCondList = tableRowCondsWithUser.get(username);
        for (String tbl : tableRowCondList.keySet()) {
            Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, tbl));
            Map<String, List<RowACL.Cond>> condsWithColumn = tableRowCondList.getRowCondListByTable(tbl).getCondsWithColumn();
            result.put(tbl, concatConds(condsWithColumn, columnWithType));
        }
        return result;
    }

    public String preview(String project, String table, Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return concatConds(condsWithColumn, columnWithType);
    }

    public void addRowACL(String project, String username, String table, Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).add(username, table, condsWithColumn);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void updateRowACL(String project, String username, String table, Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).update(username, table, condsWithColumn);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACL(String project, String username, String table) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).delete(username, table);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACL(String project, String username) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).deleteByUser(username);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACLByTbl(String project, String table) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).deleteByTbl(table);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    private Map<String, String> getColumnWithType(String project, String table) {
        Map<String, String> columnWithType = new HashMap<>();
        TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(table, project);
        ColumnDesc[] columns = tableDesc.getColumns();
        for (ColumnDesc column : columns) {
            columnWithType.put(column.getName(), column.getTypeName());
        }
        return columnWithType;
    }

    public static String concatConds(Map<String, List<RowACL.Cond>> condsWithCol, Map<String, String> columnWithType) {
        StrBuilder result = new StrBuilder();
        int j = 0;
        for (String col : condsWithCol.keySet()) {
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type not found");
            List<RowACL.Cond> conds = condsWithCol.get(col);
            for (int i = 0; i < conds.size(); i++) {
                String parsedCond = conds.get(i).toString(col, type);
                if (conds.size() == 1) {
                    result.append(parsedCond);
                    continue;
                }
                if (i == 0) {
                    result.append("(").append(parsedCond).append(" OR ");
                    continue;
                }
                if (i == conds.size() - 1) {
                    result.append(parsedCond).append(")");
                    continue;
                }
                result.append(parsedCond);
            }
            if (j != condsWithCol.size() - 1) {
                result.append(" AND ");
            }
            j++;
        }
        return result.toString();
    }
}
