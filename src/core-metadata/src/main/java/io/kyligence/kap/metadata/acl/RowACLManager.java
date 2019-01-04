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

import static io.kyligence.kap.metadata.acl.ColumnToConds.concatConds;
import static io.kyligence.kap.metadata.acl.ColumnToConds.getColumnWithType;
import static org.apache.kylin.metadata.MetadataConstants.TYPE_GROUP;
import static org.apache.kylin.metadata.MetadataConstants.TYPE_USER;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;

public class RowACLManager {

    private static final Logger logger = LoggerFactory.getLogger(RowACLManager.class);

    public static RowACLManager getInstance(KylinConfig config) {
        return config.getManager(RowACLManager.class);
    }

    // called by reflection
    static RowACLManager newInstance(KylinConfig config) throws IOException {
        return new RowACLManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private ResourceStore store;
    private static final Serializer<RowACL> ROW_ACL_SERIALIZER = new JsonSerializer<>(RowACL.class);
    private static final String DIR_PREFIX = "/row_acl";
    private Map<String, String> rowACLKeys;
    private Cache<String, Map<String, String>> queryUserRowACL;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    public RowACLManager(KylinConfig config) throws IOException {
        this.config = config;
        logger.info("Initializing LegacyRowACLManager with config " + config);
        this.store = ResourceStore.getKylinMetaStore(config);
        this.queryUserRowACL = CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(3, TimeUnit.DAYS).build();

        // todo prj
        this.rowACLKeys = new ConcurrentSkipListMap<>();
        loadAllKeys();
    }

    private void loadAllKeys() throws IOException {
        NavigableSet<String> keys = store.listResourcesRecursively(DIR_PREFIX);
        if (keys == null) {
            return;
        }
        for (String key : keys) {
            rowACLKeys.put(key, "");
        }
    }

    //table:concatedCond, for backend query.
    public Map<String, String> getConcatCondsByEntity(String project, String type, String name) {
        Map<String, String> userRowACL = queryUserRowACL.getIfPresent(name);
        if (userRowACL == null) {
            logger.info("Can not get " + name + "'row ACL from cache, ask metastore.");

            userRowACL = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            Collection<String> tables = getParsedKeysByPrj(project).entityToTable.get(type, name);

            for (String tbl : tables) {
                Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, tbl));
                RowACL rowACL = Preconditions.checkNotNull(getRowACL(project, type, name, tbl),
                        "Something wrong with parse Row ACL keys.");
                userRowACL.put(tbl, concatConds(rowACL.getColumnToConds(), columnWithType));
            }
            queryUserRowACL.put(name, userRowACL);
            logger.info("Finish load " + name + "'row ACL.");
        }
        return userRowACL;
    }

    //user/group:ColumnToConds, for frontend display.
    public Map<String, ColumnToConds> getRowACLByTable(String project, String type, String table) throws IOException {
        table = table.toUpperCase();
        Map<String, ColumnToConds> r = new HashMap<>();
        Collection<String> entities = getParsedKeysByPrj(project).tableToEntity.get(type, table);
        for (String e : entities) {
            RowACL acl = Preconditions.checkNotNull(getRowACL(project, type, e, table),
                    "Something wrong with parse Row ACL keys.");
            r.put(e, acl.getColumnToConds());
        }
        return r;
    }

    public Collection<String> getRowACLEntitesByTable(String project, String type, String table) {
        return getParsedKeysByPrj(project).tableToEntity.get(type, table.toUpperCase());
    }

    public RowACL getRowACL(String project, String type, String name, String table) {
        table = table.toUpperCase();
        RowACL acl;
        acl = store.getResource(path(project, type, name, table), ROW_ACL_SERIALIZER);
        return acl;
    }

    public void addRowACL(String project, String type, String name, String table, ColumnToConds condsWithColumn)
            throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            validateACLNotExists(project, type, name, table);
            RowACL rowACL = newRowACL(project, type, name, table);
            putRowACL(project, type, name, table, condsWithColumn, rowACL);
        }
    }

    public void updateRowACL(String project, String type, String name, String table, ColumnToConds condsWithColumn)
            throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            table = table.toUpperCase();
            validateACLExists(project, type, name, table);
            RowACL rowACL = store.getResource(path(project, type, name, table), ROW_ACL_SERIALIZER);
            putRowACL(project, type, name, table, condsWithColumn, rowACL);
        }
    }

    private void putRowACL(String project, String type, String name, String table, ColumnToConds condsWithColumn,
            RowACL rowACL) throws IOException {
        rowACL.add(condsWithColumn);
        store.checkAndPutResource(path(project, type, name, table), rowACL, ROW_ACL_SERIALIZER);
        rowACLKeys.put(path(project, type, name, table), "");
    }

    public void deleteRowACL(String project, String type, String name, String table) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            table = table.toUpperCase();
            validateACLExists(project, type, name, table);
            store.deleteResource(path(project, type, name, table));
            rowACLKeys.remove(path(project, type, name, table));
        }
    }

    public void deleteRowACLByTbl(String project, String table) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            table = table.toUpperCase();
            delete(project, TYPE_USER, table);
            delete(project, TYPE_GROUP, table);
        }
    }

    private void delete(String project, String type, String table) throws IOException {
        table = table.toUpperCase();
        Collection<String> entities = getParsedKeysByPrj(project).tableToEntity.get(type, table);
        for (String e : entities) {
            store.deleteResource(path(project, type, e, table));
            rowACLKeys.remove(path(project, type, e, table));
        }
    }

    public void deleteRowACL(String project, String type, String name) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            Collection<String> tables = getParsedKeysByPrj(project).entityToTable.get(type, name);
            for (String t : tables) {
                store.deleteResource(path(project, type, name, t));
                rowACLKeys.remove(path(project, type, name, t));
            }
        }
    }

    private RowACL newRowACL(String project, String type, String name, String table) {
        table = table.toUpperCase();
        RowACL acl = new RowACL();
        acl.init(project, type, name, table);
        return acl;
    }

    @Nonnull
    private BiParsedKeys getParsedKeysByPrj(String project) {
        BiParsedKeys parsedKeys = new BiParsedKeys();
        Set<String> keys = this.rowACLKeys.keySet();
        for (String key : keys) {
            //key : /row_acl/project/type/userOrGroup/table.json
            String[] paths = key.split("/");
            Preconditions.checkArgument(paths.length == 6);

            String prj = paths[2];
            if (!prj.equalsIgnoreCase(project)) {
                continue;
            }

            String type = paths[3];
            String username = paths[4];
            String table = paths[5].split("\\.json")[0];
            parsedKeys.update(type, username, table);
        }
        return parsedKeys;
    }

    private static String path(String project, String type, String name, String table) {
        return DIR_PREFIX + "/" + project + "/" + type + "/" + name + "/" + table.toUpperCase() + ".json";
    }

    private void validateACLExists(String project, String type, String name, String table) throws IOException {
        if (!store.exists(path(project, type, name, table))) {
            throw new RuntimeException("Operation fail, table:" + table + " not have any row acl conds!");
        }
    }

    private void validateACLNotExists(String project, String type, String name, String table) throws IOException {
        if (store.exists(path(project, type, name, table))) {
            throw new RuntimeException("Operation fail, entity:" + name + ", table:" + table + " already has row ACL!");
        }
    }

    private static class BiParsedKeys {
        private ParsedKeys entityToTable = new ParsedKeys();
        private ParsedKeys tableToEntity = new ParsedKeys();

        public void update(String type, String username, String table) {
            if (type.equals(TYPE_USER)) {
                entityToTable.user.put(username, table);
                tableToEntity.user.put(table, username);
            } else {
                entityToTable.group.put(username, table);
                tableToEntity.group.put(table, username);
            }
        }
    }

    private static class ParsedKeys {
        // May be userOrGroup <=> tables OR tables <=> userOrGroup
        private ArrayListMultimap<String, String> user = ArrayListMultimap.create();
        private ArrayListMultimap<String, String> group = ArrayListMultimap.create();

        @Nonnull
        private List<String> get(String type, String name) {
            List<String> r;
            if (type.equals(TYPE_USER)) {
                r = user.get(name);
            } else {
                r = group.get(name);
            }

            if (r == null) {
                r = Collections.emptyList();
            }
            return r;
        }
    }
}
