/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class NTableMetadataManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NTableMetadataManager.class);

    public static NTableMetadataManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NTableMetadataManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NTableMetadataManager newInstance(KylinConfig config, String project) {
        return new NTableMetadataManager(config, project);
    }

    // ============================================================================

    private final KylinConfig config;
    private final String project;

    private CachedCrudAssist<TableDesc> srcTableCrud;
    private CachedCrudAssist<TableExtDesc> srcExtCrud;

    private NTableMetadataManager(KylinConfig cfg, String project) {
        this.config = cfg;
        this.project = project;

        initSrcTable();
        initSrcExt();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // ============================================================================
    // TableDesc methods
    // ============================================================================

    private void initSrcTable() {
        String resourceRootPath = "/" + project + ResourceStore.TABLE_RESOURCE_ROOT;
        this.srcTableCrud = new CachedCrudAssist<TableDesc>(getStore(), resourceRootPath, TableDesc.class) {
            @Override
            protected TableDesc initEntityAfterReload(TableDesc t, String resourceName) {
                t.init(project);
                return t;
            }
        };
    }

    public void invalidCache(String resourceName) {
        srcTableCrud.invalidateCache(resourceName);
        srcExtCrud.invalidateCache(resourceName);
    }

    public List<TableDesc> listAllTables() {
        return srcTableCrud.listAll();
    }

    public Map<String, List<TableDesc>> listTablesGroupBySchema() {
        return listAllTables().stream().collect(groupingBy(TableDesc::getDatabase));
    }

    public Map<String, List<TableDesc>> dbToTablesMap(boolean streamingEnabled) {
        return listAllTables().stream() //
                .filter(table -> table.isAccessible(streamingEnabled)) //
                .collect(groupingBy(table -> table.getDatabase().toUpperCase(Locale.ROOT), Collectors.toList()));
    }

    public Map<String, TableDesc> getAllTablesMap() {
        Map<String, TableDesc> ret = new LinkedHashMap<>();
        for (TableDesc table : listAllTables()) {
            ret.put(table.getIdentity(), table);
        }
        return ret;
    }

    public List<TableDesc> getAllIncrementalLoadTables() {
        List<TableDesc> result = Lists.newArrayList();

        for (TableDesc table : srcTableCrud.listAll()) {
            if (table.isIncrementLoading())
                result.add(table);
        }

        return result;
    }

    /**
     * Get TableDesc by name and project
     */
    public TableDesc getTableDesc(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        return srcTableCrud.get(tableName);
    }

    public TableDesc copy(TableDesc tableDesc) {
        return srcTableCrud.copyBySerialization(tableDesc);
    }

    public TableDesc copyForWrite(TableDesc tableDesc) {
        if (tableDesc.getProject() == null) {
            tableDesc.setProject(project);
        }
        return srcTableCrud.copyForWrite(tableDesc);
    }

    public TableExtDesc copyForWrite(TableExtDesc tableExtDesc) {
        return srcExtCrud.copyForWrite(tableExtDesc);
    }

    /**
     * @deprecated Use updateTableDesc(String tableName, TableDescUpdater updater) instead.
     */
    @Deprecated
    public void saveSourceTable(TableDesc srcTable) {
        if (srcTableCrud.contains(srcTable.getIdentity())) {
            updateTableDesc(srcTable.getIdentity(), srcTable::copyPropertiesTo);
        } else {
            createTableDesc(srcTable);
        }
    }

    public void removeSourceTable(String tableIdentity) {
        TableDesc t = getTableDesc(tableIdentity);
        if (t == null)
            return;
        srcTableCrud.delete(t);
    }

    /**
     * the project-specific table desc will be expand by computed columns from the projects' models
     * when the projects' model list changed, project-specific table should be reset and get expanded
     * again
     */
    public void resetProjectSpecificTableDesc() {
        srcTableCrud.reloadAll();
    }

    /**
     * @deprecated Use updateTableDesc(String tableName, TableDescUpdater updater) instead.
     */
    @Deprecated
    public void updateTableDesc(TableDesc tableDesc) {
        updateTableDesc(tableDesc.getIdentity(), tableDesc::copyPropertiesTo);
    }

    public void updateTableDesc(String tableName, TableDescUpdater updater) {
        TableDesc cached = getTableDesc(tableName);
        if (cached == null) {
            throw new IllegalStateException("tableDesc " + tableName + " does not exist");
        }
        TableDesc copy = copyForWrite(cached);
        updater.modify(copy);
        copy.init(project);
        srcTableCrud.save(copy);
    }
    
    public void createTableDesc(TableDesc srcTable) {
        srcTable.init(project);
        TableDesc copy = copyForWrite(srcTable);
        srcTableCrud.save(copy);
    }

    public interface TableDescUpdater {
        void modify(TableDesc copyForWrite);
    }

    // ============================================================================
    // TableExtDesc methods
    // ============================================================================

    private void initSrcExt() {
        this.srcExtCrud = new CachedCrudAssist<TableExtDesc>(getStore(),
                "/" + project + ResourceStore.TABLE_EXD_RESOURCE_ROOT, TableExtDesc.class) {
            @Override
            protected TableExtDesc initEntityAfterReload(TableExtDesc t, String resourceName) {
                // convert old tableExt json to new one
                if (t.getIdentity() == null) {
                    t = convertOldTableExtToNewer(resourceName);
                }
                t.init(project);
                return t;
            }
        };
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     */
    public TableExtDesc getOrCreateTableExt(String tableName) {
        TableDesc t = getTableDesc(tableName);
        if (t == null)
            return null;

        return getOrCreateTableExt(t);
    }

    public boolean isTableExtExist(String tableIdentity) {
        return getTableExtIfExists(getTableDesc(tableIdentity)) != null;
    }

    public TableExtDesc getOrCreateTableExt(TableDesc t) {
        TableExtDesc result = srcExtCrud.get(t.getIdentity());

        // avoid returning null, since the TableDesc exists
        if (null == result) {
            result = new TableExtDesc();
            result.setIdentity(t.getIdentity());
            result.setUuid(RandomUtil.randomUUIDStr());
            result.setLastModified(0);
            result.init(t.getProject());
        }
        return result;
    }

    public TableExtDesc getTableExtIfExists(TableDesc t) {
        return srcExtCrud.get(t.getIdentity());
    }

    // for test mostly
    public Serializer<TableDesc> getTableMetadataSerializer() {
        return srcTableCrud.getSerializer();
    }

    /**
     * @deprecated Use updateTableExt(String tableName, TableExtDescUpdater updater) instead.
     */
    @Deprecated
    public void saveTableExt(TableExtDesc tableExt) {
        if (srcExtCrud.contains(tableExt.getIdentity())) {
            updateTableExt(tableExt.getIdentity(), tableExt::copyPropertiesTo);
        } else {
            createTableExt(tableExt);
        }
    }

    public void mergeAndUpdateTableExt(TableExtDesc origin, TableExtDesc other) {
        updateTableExt(origin.getIdentity(), copyForWrite -> {
            copyForWrite.setColumnStats(other.getAllColumnStats());
            copyForWrite.setSampleRows(other.getSampleRows());
            copyForWrite.setTotalRows(other.getTotalRows());
            copyForWrite.setJodID(other.getJodID());
            if (other.getOriginalSize() != -1) {
                copyForWrite.setOriginalSize(other.getOriginalSize());
            }
        });
    }

    public void updateTableExt(String tableName, TableExtDescUpdater updater) {
        TableExtDesc cached = getOrCreateTableExt(tableName);
        TableExtDesc copy = copyForWrite(cached);
        updater.modify(copy);
        srcExtCrud.save(copy);
    }

    public void createTableExt(TableExtDesc tableExt) {
        if (tableExt.getUuid() == null || tableExt.getIdentity() == null) {
            throw new IllegalArgumentException();
        }
        TableExtDesc copy = copyForWrite(tableExt);
        srcExtCrud.save(copy);
    }

    public void saveOrUpdateTableExt(boolean isUpdate, TableExtDesc tableExt) {
        if (isUpdate) {
            mergeAndUpdateTableExt(tableExt, tableExt);
        } else {
            createTableExt(tableExt);
        }
    }

    public void removeTableExt(String tableName) {
        TableExtDesc t = getTableExtIfExists(getTableDesc(tableName));
        if (t == null)
            return;

        srcExtCrud.delete(t);
    }

    public boolean existsSnapshotTableByName(String tableName) {
        String snapshotDir = getTableDesc(tableName).getLastSnapshotPath();
        return StringUtils.isNotEmpty(snapshotDir);
    }

    private TableExtDesc convertOldTableExtToNewer(String resourceName) {
        ResourceStore store = getStore();
        Map<String, String> attrs = Maps.newHashMap();

        try {
            RawResource res = store.getResource(
                    ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + resourceName + MetadataConstants.FILE_SURFIX);

            try (InputStream is = res.getByteSource().openStream()) {
                attrs.putAll(JsonUtil.readValue(is, HashMap.class));
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }

        String cardinality = attrs.get(MetadataConstants.TABLE_EXD_CARDINALITY);

        // parse table identity from file name
        String tableIdentity = TableDesc.parseResourcePath(resourceName).getFirst();
        TableExtDesc result = new TableExtDesc();
        result.setIdentity(tableIdentity);
        result.setUuid(RandomUtil.randomUUIDStr());
        result.setLastModified(0);
        result.setCardinality(cardinality);
        return result;
    }

    public interface TableExtDescUpdater {
        void modify(TableExtDesc copyForWrite);
    }
}
