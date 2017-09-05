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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ColumnACLManager {

    private static final Logger logger = LoggerFactory.getLogger(ColumnACLManager.class);

    public static final Serializer<ColumnACL> COLUMN_ACL_SERIALIZER = new JsonSerializer<>(ColumnACL.class);
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

    public static void clearCache() {
        CACHE.clear();
    }

    public static void clearCache(KylinConfig kylinConfig) {
        if (kylinConfig != null)
            CACHE.remove(kylinConfig);
    }

    // ============================================================================

    private KylinConfig config;

    private ColumnACLManager(KylinConfig config) throws IOException {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public ColumnACL getColumnACL(String project) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL columnACLRecord = getStore().getResource(path, ColumnACL.class, COLUMN_ACL_SERIALIZER);
        if (columnACLRecord == null || columnACLRecord.getUserColumnBlackList() == null) {
            return new ColumnACL();
        }
        return columnACLRecord;
    }

    public void addColumnACL(String project, String username, String table, List<String> columns) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL tableACL = getColumnACL(project);
        getStore().putResource(path, tableACL.add(username, table, columns), System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
    }

    public void updateColumnACL(String project, String username, String table, List<String> columns) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL tableACL = getColumnACL(project);
        getStore().putResource(path, tableACL.update(username, table, columns), System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
    }

    public void deleteColumnACL(String project, String username, String table) throws IOException {
        String path = DIR_PREFIX + project;
        ColumnACL tableACL = getColumnACL(project);
        getStore().putResource(path, tableACL.delete(username, table), System.currentTimeMillis(), COLUMN_ACL_SERIALIZER);
    }
}
