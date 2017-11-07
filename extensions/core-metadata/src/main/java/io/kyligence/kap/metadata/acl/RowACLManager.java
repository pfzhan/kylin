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

import static io.kyligence.kap.metadata.acl.RowACL.concatConds;
import static io.kyligence.kap.metadata.acl.RowACL.getColumnWithType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
        if (rowACLRecord == null) {
            return new RowACL();
        }
        return rowACLRecord;
    }

    public Map<String, String> getQueryUsedTblToConds(String project, String name, String type) {
        return getRowACLByCache(project).getQueryUsedTblToConds(project, name, type);
    }

    public String preview(String project, String table, RowACL.ColumnToConds condsWithColumn)
            throws IOException {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return concatConds(condsWithColumn, columnWithType);
    }

    public void addRowACL(String project, String name, String table, RowACL.ColumnToConds condsWithColumn, String type)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).add(name, table, condsWithColumn, type);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void updateRowACL(String project, String name, String table, RowACL.ColumnToConds condsWithColumn, String type)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).update(name, table, condsWithColumn, type);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACL(String project, String name, String table, String type) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).delete(name, table, type);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACL(String project, String name, String type) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).delete(name, type);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }

    public void deleteRowACLByTbl(String project, String table) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project).deleteByTbl(table);
        getStore().putResource(path, rowACL, System.currentTimeMillis(), ROW_ACL_SERIALIZER);
        rowACLMap.put(project, rowACL);
    }
}
