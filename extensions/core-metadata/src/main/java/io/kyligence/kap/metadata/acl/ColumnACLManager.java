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
import java.util.List;
import java.util.Set;
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

/**
 */
public class ColumnACLManager {

    private static final Logger logger = LoggerFactory.getLogger(ColumnACLManager.class);

    private static final Serializer<ColumnACL> COLUMN_ACL_SERIALIZER = new JsonSerializer<>(ColumnACL.class);
    private static final String DIR_PREFIX = "/column_acl/";

    // static cached instances
    private static final ConcurrentMap<KylinConfig, ColumnACLManager> CACHE = new ConcurrentHashMap<>();

    public static ColumnACLManager getInstance(KylinConfig config) {
        ColumnACLManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (ColumnACLManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new ColumnACLManager(config);
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
    // user ==> TableACL
    private CaseInsensitiveStringCache<ColumnACL> columnACLMap;

    public ColumnACLManager(KylinConfig config) throws IOException {
        logger.info("Initializing ColumnACLManager with config " + config);
        this.config = config;
        this.columnACLMap = new CaseInsensitiveStringCache<>(config, "column_acl");
        loadAllColumnACL();
        Broadcaster.getInstance(config).registerListener(new ColumnACLSyncListener(), "column_acl");
    }

    private class ColumnACLSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            reloadColumnACL(cacheKey);
            broadcaster.notifyProjectACLUpdate(cacheKey);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public ColumnACL getColumnACLByCache(String project) {
        ColumnACL columnACL = columnACLMap.get(project);
        if (columnACL == null) {
            return new ColumnACL();
        }
        return columnACL;
    }

    private void loadAllColumnACL() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively("/column_acl", "");
        final int prefixLen = DIR_PREFIX.length();
        for (String path : paths) {
            String project = path.substring(prefixLen, path.length());
            reloadColumnACL(project);
        }
        logger.info("Loading row ACL from folder " + store.getReadableResourcePath("/column_acl"));
    }


    private void reloadColumnACL(String project) throws IOException {
        ColumnACL tableACLRecord = getColumnACL(project);
        columnACLMap.putLocal(project, tableACLRecord);
    }

    private ColumnACL getColumnACL(String project) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACLRecord = getStore().getResource(path, ColumnACL.class, COLUMN_ACL_SERIALIZER);
        if (columnACLRecord == null || columnACLRecord.getUserColumnBlackList() == null) {
            return new ColumnACL();
        }
        return columnACLRecord;
    }

    public void addColumnACL(String project, String username, String table, Set<String> columns) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACL = getColumnACL(project).add(username, table, columns);
        getStore().putResource(path, columnACL, System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
        columnACLMap.put(project, columnACL);
    }

    public void updateColumnACL(String project, String username, String table, Set<String> columns) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACL = getColumnACL(project).update(username, table, columns);
        getStore().putResource(path, columnACL, System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
        columnACLMap.put(project, columnACL);
    }

    public void deleteColumnACL(String project, String username, String table) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACL = getColumnACL(project).delete(username, table);
        getStore().putResource(path, columnACL, System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
        columnACLMap.put(project, columnACL);
    }

    public void deleteColumnACL(String project, String username) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACL = getColumnACL(project).delete(username);
        getStore().putResource(path, columnACL, System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
        columnACLMap.put(project, columnACL);
    }

    public void deleteColumnACLByTbl(String project, String table) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACL = getColumnACL(project).deleteByTbl(table);
        getStore().putResource(path, columnACL, System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
        columnACLMap.put(project, columnACL);
    }
}
