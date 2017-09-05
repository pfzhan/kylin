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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class RowACLManager {

    private static final Logger logger = LoggerFactory.getLogger(RowACLManager.class);

    public static final Serializer<RowACL> ROW_ACL_SERIALIZER = new JsonSerializer<>(RowACL.class);
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

    public static void clearCache() {
        CACHE.clear();
    }

    public static void clearCache(KylinConfig kylinConfig) {
        if (kylinConfig != null)
            CACHE.remove(kylinConfig);
    }

    // ============================================================================

    private KylinConfig config;

    private RowACLManager(KylinConfig config) throws IOException {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public RowACL getRowACL(String project) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACLRecord = getStore().getResource(path, RowACL.class, ROW_ACL_SERIALIZER);
        if (rowACLRecord == null || rowACLRecord.getTableRowCondsWithUser() == null) {
            return new RowACL();
        }
        return rowACLRecord;
    }

    public String preview(String project, String table, Map<String, List<String>> condsWithColumn)
            throws IOException {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return RowACL.concatConds(condsWithColumn, columnWithType);
    }

    public void addRowACL(String project, String username, String table, Map<String, List<String>> condsWithColumn)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project);
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        getStore().putResource(path, rowACL.add(username, table, condsWithColumn, columnWithType), System.currentTimeMillis(),
                ROW_ACL_SERIALIZER);
    }

    public void updateRowACL(String project, String username, String table, Map<String, List<String>> condsWithColumn)
            throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project);
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        getStore().putResource(path, rowACL.update(username, table, condsWithColumn, columnWithType), System.currentTimeMillis(),
                ROW_ACL_SERIALIZER);
    }

    public void deleteRowACL(String project, String username, String table) throws IOException {
        String path = DIR_PREFIX + project;
        RowACL rowACL = getRowACL(project);
        getStore().putResource(path, rowACL.delete(username, table), System.currentTimeMillis(), ROW_ACL_SERIALIZER);
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
}
