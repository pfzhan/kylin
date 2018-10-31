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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.NProjectManager;

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
    static NTableMetadataManager newInstance(KylinConfig config, String project)
            throws IOException, ClassNotFoundException {
        return new NTableMetadataManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    // table name ==> SourceTable
    private CaseInsensitiveStringCache<TableDesc> srcTableMap;
    private CachedCrudAssist<TableDesc> srcTableCrud;
    private AutoReadWriteLock srcTableMapLock = new AutoReadWriteLock();

    // name => SourceTableExt
    private CaseInsensitiveStringCache<TableExtDesc> srcExtMap;
    private CachedCrudAssist<TableExtDesc> srcExtCrud;
    private AutoReadWriteLock srcExtMapLock = new AutoReadWriteLock();

    // name => ExternalFilterDesc
    private CaseInsensitiveStringCache<ExternalFilterDesc> extFilterMap;
    private CachedCrudAssist<ExternalFilterDesc> extFilterCrud;
    private AutoReadWriteLock extFilterMapLock = new AutoReadWriteLock();

    //    private NTableMetadataManager(KylinConfig cfg) throws IOException, ClassNotFoundException {
    //        this(cfg, null);
    //    }

    private NTableMetadataManager(KylinConfig cfg, String project) throws IOException, ClassNotFoundException {
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

    private void initSrcTable() throws IOException, ClassNotFoundException {
        this.srcTableMap = new CaseInsensitiveStringCache<>(config, project, "table");
        String resourceRootPath = "/" + project + ResourceStore.TABLE_RESOURCE_ROOT;
        this.srcTableCrud = new CachedCrudAssist<TableDesc>(getStore(), resourceRootPath,
                // TODO: ugly casting here
                (Class<TableDesc>) (Class.forName(NTableDesc.class.getName())), srcTableMap) {
            @Override
            protected TableDesc initEntityAfterReload(TableDesc t, String resourceName) {
                t.init(project);
                return t;
            }
        };
        srcTableCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new SrcTableSyncListener(), project, "table");
    }

    private class SrcTableSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock ignored = srcTableMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    srcTableMap.removeLocal(cacheKey);
                else
                    srcTableCrud.reloadQuietly(cacheKey);
            }

            Pair<String, String> pair = TableDesc.parseResourcePath(cacheKey);
            String table = pair.getFirst();
            String prj = pair.getSecond();

            if (prj == null) {
                for (ProjectInstance p : getProjectManager().findProjectsByTable(table)) {
                    broadcaster.notifyProjectSchemaUpdate(p.getName());
                }
            } else {
                broadcaster.notifyProjectSchemaUpdate(prj);
            }
        }
    }

    public List<TableDesc> listAllTables() {
        try (AutoLock ignored = srcTableMapLock.lockForWrite()) {
            return Lists.newArrayList(getAllTablesMap().values());
        }
    }

    public Map<String, TableDesc> getAllTablesMap() {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            ProjectInstance pi = getProjectManager().getProject(project);
            Set<String> prjTableNames = pi.getTables();

            Map<String, TableDesc> ret = new LinkedHashMap<>();
            for (String tableName : prjTableNames) {
                String tableIdentity = getTableIdentity(tableName);
                ret.put(tableIdentity, getTableDesc(tableIdentity));
            }
            return ret;
        }
    }

    /**
     * Get TableDesc by name and project
     */
    public TableDesc getTableDesc(String tableName) {
        try (AutoLock ignored = srcTableMapLock.lockForRead()) {
            return srcTableMap.get(tableName);
        }
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

    public void saveSourceTable(TableDesc srcTable) throws IOException {
        try (AutoLock ignored = srcTableMapLock.lockForWrite()) {
            srcTable.init(project);
            srcTableCrud.save(srcTable);
            getProjectManager().addTableDescToProject(new String[] { srcTable.getIdentity() }, project);
        }
    }

    public void removeSourceTable(String tableIdentity) throws IOException {
        try (AutoLock ignored = srcTableMapLock.lockForWrite()) {
            TableDesc t = getTableDesc(tableIdentity);
            if (t == null)
                return;
            getProjectManager().removeTableDescFromProject(tableIdentity, project);
            srcTableCrud.delete(t);
        }
    }

    /**
     * the project-specific table desc will be expand by computed columns from the projects' models
     * when the projects' model list changed, project-specific table should be reset and get expanded
     * again
     */
    public void resetProjectSpecificTableDesc() {
        try (AutoLock ignored = srcTableMapLock.lockForWrite()) {
            ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(project);
            for (String tableName : projectInstance.getTables()) {
                String tableIdentity = getTableIdentity(tableName);
                //                String key = resourcePath(prj, tableIdentity);
                TableDesc originTableDesc = srcTableMap.get(tableIdentity);
                if (originTableDesc == null) {
                    continue;
                }
                srcTableCrud.reloadQuietly(tableIdentity);
            }
        }
    }

    // ============================================================================
    // TableExtDesc methods
    // ============================================================================

    private void initSrcExt() throws IOException, ClassNotFoundException {
        this.srcExtMap = new CaseInsensitiveStringCache<>(config, project, "table_ext");
        this.srcExtCrud = new CachedCrudAssist<TableExtDesc>(getStore(),
                "/" + project + ResourceStore.TABLE_EXD_RESOURCE_ROOT,
                (Class<TableExtDesc>) (Class.forName(NTableExtDesc.class.getName())), srcExtMap) {
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
        Broadcaster.getInstance(config).registerListener(new SrcTableExtSyncListener(), project, "table_ext");
    }

    private class SrcTableExtSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) {
            try (AutoLock ignored = srcExtMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    srcExtMap.removeLocal(cacheKey);
                else
                    srcExtCrud.reloadQuietly(cacheKey);
            }
        }
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     *
     * @param tableName
     * @return
     */
    public TableExtDesc getTableExt(String tableName) {
        TableDesc t = getTableDesc(tableName);
        if (t == null)
            return null;

        return getTableExt(t);
    }

    public TableExtDesc getTableExt(TableDesc t) {
        try (AutoLock ignored = srcExtMapLock.lockForRead()) {
            TableExtDesc result = srcExtMap.get(t.getIdentity());

            // avoid returning null, since the TableDesc exists
            if (null == result) {
                result = new NTableExtDesc();
                result.setIdentity(t.getIdentity());
                result.setUuid(UUID.randomUUID().toString());
                result.setLastModified(0);
                result.init(t.getProject());

                // TODO: No need write lock? -- ETHER
                srcExtMap.put(t.getIdentity(), result);
            }
            return result;
        }
    }

    public void saveTableExt(TableExtDesc tableExt) throws IOException {
        try (AutoLock ignored = srcExtMapLock.lockForWrite()) {
            if (tableExt.getUuid() == null || tableExt.getIdentity() == null) {
                throw new IllegalArgumentException();
            }

            // what is this doing??
            String path = tableExt.getResourcePath();
            ResourceStore store = getStore();
            TableExtDesc t = store.getResource(path, TableExtDesc.class, TABLE_EXT_SERIALIZER);
            if (t != null && t.getIdentity() == null)
                store.deleteResource(path);

            srcExtCrud.save(tableExt);
        }
    }

    public void removeTableExt(String tableName) throws IOException {
        try (AutoLock ignored = srcExtMapLock.lockForWrite()) {
            // note, here assume always delete TableExtDesc first, then TableDesc
            TableExtDesc t = getTableExt(tableName);
            if (t == null)
                return;

            srcExtCrud.delete(t);
        }
    }

    private TableExtDesc convertOldTableExtToNewer(String resourceName) {
        ResourceStore store = getStore();
        Map<String, String> attrs = Maps.newHashMap();

        try {
            RawResource res = store.getResource(
                    ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + resourceName + MetadataConstants.FILE_SURFIX);

            InputStream is = res.inputStream;
            try {
                attrs.putAll(JsonUtil.readValue(is, HashMap.class));
            } finally {
                if (is != null)
                    is.close();
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

    private void initExtFilter() throws IOException {
        this.extFilterMap = new CaseInsensitiveStringCache<>(config, project, "external_filter");
        this.extFilterCrud = new CachedCrudAssist<ExternalFilterDesc>(getStore(),
                ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT, ExternalFilterDesc.class, extFilterMap) {
            @Override
            protected ExternalFilterDesc initEntityAfterReload(ExternalFilterDesc t, String resourceName) {
                return t; // noop
            }
        };
        extFilterCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new ExtFilterSyncListener(), project, "external_filter");
    }

    private class ExtFilterSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) {
            try (AutoLock ignored = extFilterMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    extFilterMap.removeLocal(cacheKey);
                else
                    extFilterCrud.reloadQuietly(cacheKey);
            }
        }
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        try (AutoLock ignored = extFilterMapLock.lockForRead()) {
            return Lists.newArrayList(extFilterMap.values());
        }
    }

    public ExternalFilterDesc getExtFilterDesc(String filterTableName) {
        try (AutoLock ignored = extFilterMapLock.lockForRead()) {
            ExternalFilterDesc result = extFilterMap.get(filterTableName);
            return result;
        }
    }

    public void saveExternalFilter(ExternalFilterDesc desc) throws IOException {
        try (AutoLock ignored = extFilterMapLock.lockForWrite()) {
            extFilterCrud.save(desc);
        }
    }
    public void updateTableDesc(TableDesc tableDesc) throws IOException {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            if(!srcTableMap.containsKey(tableDesc.getIdentity())){
                throw new IllegalStateException("tableDesc " + tableDesc.getName() + "does not exist");
            }
            saveSourceTable(tableDesc);
        }
    }

    public void removeExternalFilter(String name) throws IOException {
        try (AutoLock ignored = extFilterMapLock.lockForWrite()) {
            extFilterCrud.delete(name);
        }
    }

    private NProjectManager getProjectManager() {
        return NProjectManager.getInstance(config);
    }
}
