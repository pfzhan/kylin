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

package io.kyligence.kap.metadata.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 */
public class NTableMetadataManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NTableMetadataManager.class);

    private static final Serializer<TableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<>(TableExtDesc.class);

    public static NTableMetadataManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NTableMetadataManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NTableMetadataManager newInstance(KylinConfig config, String project) {
        return new NTableMetadataManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<TableDesc> srcTableCrud;
    private CachedCrudAssist<TableExtDesc> srcExtCrud;
    private CachedCrudAssist<ExternalFilterDesc> extFilterCrud;

    private NTableMetadataManager(KylinConfig cfg, String project) {
        this.config = cfg;
        this.project = project;

        initSrcTable();
        initSrcExt();
        initExtFilter();
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
        srcTableCrud.reloadAll();
    }

    public List<TableDesc> listAllTables() {
        return srcTableCrud.listAll();
    }

    public Map<String, List<TableDesc>> listTablesGroupBySchema() {
        return listAllTables().stream().collect(groupingBy(TableDesc::getDatabase));
    }

    public Set<String> listDatabases() {
        return listAllTables().stream().map(TableDesc::getDatabase).map(String::toUpperCase)
                .collect(Collectors.toSet());
    }

    public Map<String, TableDesc> getAllTablesMap() {
        //        ProjectInstance pi = getProjectManager().getProject(project);
        //        Set<String> prjTableNames = pi.getTables();

        Map<String, TableDesc> ret = new LinkedHashMap<>();
        for (TableDesc table : listAllTables()) {
            String tableIdentity = table.getIdentity();
            ret.put(tableIdentity, getTableDesc(tableIdentity));
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

    public TableDesc copyForWrite(TableDesc tableDesc) {
        return srcTableCrud.copyForWrite(tableDesc);
    }

    public TableExtDesc copyForWrite(TableExtDesc tableExtDesc) {
        return srcExtCrud.copyForWrite(tableExtDesc);
    }

    /**
     * some legacy table name may not have DB prefix
     */
    private String getTableIdentity(String tableName) {
        if (!tableName.contains("."))
            return "DEFAULT." + tableName.toUpperCase();
        else
            return tableName.toUpperCase();
    }

    public void saveSourceTable(TableDesc srcTable) {
        srcTable.init(project);
        srcTableCrud.save(srcTable);
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
        srcExtCrud.reloadAll();
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     *
     * @param tableName
     * @return
     */
    public TableExtDesc getOrCreateTableExt(String tableName) {
        TableDesc t = getTableDesc(tableName);
        if (t == null)
            return null;

        return getOrCreateTableExt(t);
    }

    public TableExtDesc getOrCreateTableExt(TableDesc t) {
        TableExtDesc result = srcExtCrud.get(t.getIdentity());

        // avoid returning null, since the TableDesc exists
        if (null == result) {
            result = new TableExtDesc();
            result.setIdentity(t.getIdentity());
            result.setUuid(UUID.randomUUID().toString());
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

    public void saveTableExt(TableExtDesc tableExt) {
        if (tableExt.getUuid() == null || tableExt.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        // what is this doing??
        String path = tableExt.getResourcePath();
        ResourceStore store = getStore();
        TableExtDesc t = store.getResource(path, TABLE_EXT_SERIALIZER);
        if (t != null && t.getIdentity() == null)
            store.deleteResource(path);

        srcExtCrud.save(tableExt);
    }

    public void mergeAndUpdateTableExt(TableExtDesc origin, TableExtDesc other) {
        val copyForWrite = srcExtCrud.copyForWrite(origin);
        copyForWrite.setColumnStats(other.getAllColumnStats());
        copyForWrite.setSampleRows(other.getSampleRows());
        copyForWrite.setTotalRows(other.getTotalRows());
        copyForWrite.setJodID(other.getJodID());
        copyForWrite.setOriginalSize(other.getOriginalSize());
        saveTableExt(copyForWrite);
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
            throw new RuntimeException(ex);
        }

        String cardinality = attrs.get(MetadataConstants.TABLE_EXD_CARDINALITY);

        // parse table identity from file name
        String tableIdentity = TableDesc.parseResourcePath(resourceName).getFirst();
        TableExtDesc result = new TableExtDesc();
        result.setIdentity(tableIdentity);
        result.setUuid(UUID.randomUUID().toString());
        result.setLastModified(0);
        result.setCardinality(cardinality);
        return result;
    }

    // ============================================================================
    // ExternalFilterDesc methods
    // ============================================================================

    private void initExtFilter() {
        this.extFilterCrud = new CachedCrudAssist<ExternalFilterDesc>(getStore(),
                ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT, ExternalFilterDesc.class) {
            @Override
            protected ExternalFilterDesc initEntityAfterReload(ExternalFilterDesc t, String resourceName) {
                return t; // noop
            }
        };
        extFilterCrud.reloadAll();
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        return extFilterCrud.listAll();
    }

    public ExternalFilterDesc getExtFilterDesc(String filterTableName) {
        ExternalFilterDesc result = extFilterCrud.get(filterTableName);
        return result;
    }

    public void saveExternalFilter(ExternalFilterDesc desc) {
        extFilterCrud.save(desc);
    }

    public void updateTableDesc(TableDesc tableDesc) {
        if (!srcTableCrud.contains(tableDesc.getIdentity())) {
            throw new IllegalStateException("tableDesc " + tableDesc.getName() + "does not exist");
    }
        saveSourceTable(tableDesc);
    }

    public void removeExternalFilter(String name) {
        extFilterCrud.delete(name);
    }

    private NProjectManager getProjectManager() {
        return NProjectManager.getInstance(config);
    }
}