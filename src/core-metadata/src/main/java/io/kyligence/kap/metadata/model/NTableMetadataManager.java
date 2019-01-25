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
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 */
public class NTableMetadataManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NTableMetadataManager.class);

    private static final HLLCSerializer HLLC_SERIALIZER = new HLLCSerializer(DataType.getType("hllc14"));

    private static final Serializer<NTableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<>(NTableExtDesc.class);

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
    private CachedCrudAssist<NTableExtDesc> srcExtCrud;
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
        return srcTableCrud.get(tableName);
    }

    public TableDesc copyForWrite(TableDesc tableDesc) {
        return srcTableCrud.copyForWrite(tableDesc);
    }

    public NTableExtDesc copyForWrite(NTableExtDesc tableExtDesc) {
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
        this.srcExtCrud = new CachedCrudAssist<NTableExtDesc>(getStore(),
                "/" + project + ResourceStore.TABLE_EXD_RESOURCE_ROOT, NTableExtDesc.class) {
            @Override
            protected NTableExtDesc initEntityAfterReload(NTableExtDesc t, String resourceName) {
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
    public NTableExtDesc getOrCreateTableExt(String tableName) {
        TableDesc t = getTableDesc(tableName);
        if (t == null)
            return null;

        return getOrCreateTableExt(t);
    }

    public NTableExtDesc getOrCreateTableExt(TableDesc t) {
        NTableExtDesc result = srcExtCrud.get(t.getIdentity());

        // avoid returning null, since the TableDesc exists
        if (null == result) {
            result = new NTableExtDesc();
            result.setIdentity(t.getIdentity());
            result.setUuid(UUID.randomUUID().toString());
            result.setLastModified(0);
            result.init(t.getProject());
        }
        return result;
    }

    public NTableExtDesc getTableExtIfExists(TableDesc t) {
        return srcExtCrud.get(t.getIdentity());
    }

    public void saveTableExt(NTableExtDesc tableExt) {
        if (tableExt.getUuid() == null || tableExt.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        // what is this doing??
        String path = tableExt.getResourcePath();
        ResourceStore store = getStore();
        NTableExtDesc t = store.getResource(path, TABLE_EXT_SERIALIZER);
        if (t != null && t.getIdentity() == null)
            store.deleteResource(path);

        ColumnStatsStore.getInstance(tableExt, config).save();
        srcExtCrud.save(tableExt);
    }

    public void mergeAndUpdateTableExt(NTableExtDesc origin, TableExtDesc other) {
        val copyForWrite = srcExtCrud.copyForWrite(origin);
        final boolean isAppend = copyForWrite.getLoadingRange().size() < other.getLoadingRange().size();
        if (isAppend) {
            // TODO merge new range if refresh
            copyForWrite.setLoadingRange(other.getLoadingRange());
            copyForWrite.setTotalRows(other.getTotalRows());
        }

        copyForWrite.setColumnStats(other.getColumnStats());
        copyForWrite.setColStatsPath(other.getColStatsPath());
        saveTableExt(copyForWrite);
    }

    public void removeTableExt(String tableName) {
        // note, here assume always delete TableExtDesc first, then TableDesc
        NTableExtDesc t = getTableExtIfExists(getTableDesc(tableName));
        if (t == null)
            return;

        srcExtCrud.delete(t);
        ColumnStatsStore.getInstance(t, config).delete();
    }

    private NTableExtDesc convertOldTableExtToNewer(String resourceName) {
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
        NTableExtDesc result = new NTableExtDesc();
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

    public static class ColumnStatsStore {
        private final TableExtDesc tableExtDesc;
        private final KylinConfig config;
        @VisibleForTesting
        FileSystem fs;

        private ColumnStatsStore(TableExtDesc tableExtDesc, KylinConfig config) {
            this.tableExtDesc = tableExtDesc;
            this.config = config;
            fs = HadoopUtil.getWorkingFileSystem();
        }

        public static ColumnStatsStore getInstance(TableExtDesc tableExtDesc, KylinConfig config) {
            return new ColumnStatsStore(tableExtDesc, config);
        }

        public static ColumnStatsStore getInstance(TableExtDesc tableExtDesc) {
            return new ColumnStatsStore(tableExtDesc, KylinConfig.getInstanceFromEnv());
        }

        public void load() {
            if (StringUtils.isBlank(tableExtDesc.getColStatsPath())) {
                return;
            }
            FSDataInputStream in = null;
            val colStatsPath = new Path(tableExtDesc.getColStatsPath());
            try {
                if (!fs.exists(colStatsPath)) {
                    logger.error("column stats file [{}] no exists in HDFS", colStatsPath);
                    return;
                }
                in = fs.open(colStatsPath);
                final Map<String, Map<String, byte[]>> colStatsTable = JsonUtil.readValue(IOUtils.toString(in),
                        new TypeReference<Map<String, Map<String, byte[]>>>() {
                        });
                for (val colStats : tableExtDesc.getColumnStats()) {
                    val colName = colStats.getColumnName();
                    val rangeHLLC = colStatsTable.get(colName);
                    if (rangeHLLC == null || rangeHLLC.isEmpty()) {
                        continue;
                    }
                    for (val segRange : rangeHLLC.keySet()) {
                        final byte[] hllcBytes = rangeHLLC.get(segRange);
                        val hllc = HLLC_SERIALIZER.deserialize(ByteBuffer.wrap(hllcBytes));
                        colStats.addRangeHLLC(segRange, hllc);
                    }

                    colStats.init();
                }
                logger.info("load column stats file [{}] from HDFS successful", colStatsPath);
            } catch (Exception e) {
                logger.error("load column stats file [{}] from HDFS failed, please refresh this segment", colStatsPath,
                        e);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }

        public void save() {
            final Map<String, Map<String, byte[]>> colStatsTable = Maps.newHashMap();
            FSDataOutputStream out = null;
            Path newColStatsPath = null;
            try {
                for (val colStats : tableExtDesc.getColumnStats()) {
                    val colName = colStats.getColumnName();
                    colStatsTable.put(colName, Maps.newHashMap());
                    for (val rangeHLLC : colStats.getRangeHLLC().entrySet()) {
                        val segRange = rangeHLLC.getKey();
                        val hllc = rangeHLLC.getValue();
                        final ByteBuffer buffer = ByteBuffer.allocate(hllc.maxLength());
                        HLLC_SERIALIZER.serialize(hllc, buffer);
                        colStatsTable.get(colName).put(segRange, buffer.array());
                    }
                }

                newColStatsPath = new Path(getColumnStatsPath(), UUID.randomUUID().toString());
                out = fs.create(newColStatsPath, true);
                IOUtils.copy(ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(colStatsTable)).openStream(), out);
                val oldColStatsPath = tableExtDesc.getColStatsPath();
                tableExtDesc.setColStatsPath(newColStatsPath.toString());
                logger.info("update column stats file from [{}] to [{}] in HDFS successful", oldColStatsPath,
                        newColStatsPath);

                if (StringUtils.isNotBlank(oldColStatsPath) && fs.exists(new Path(oldColStatsPath))) {
                    fs.delete(new Path(oldColStatsPath), true);
                    logger.info("delete old column stats file [{}] in HDFS", oldColStatsPath);
                }

                // checking and warnning
                val size = fs.listStatus(new Path(getColumnStatsPath())).length;
                if (size > 1) {
                    logger.warn(
                            "found {} column stats files in [{}] on HDFS. This may be due to multiple parallel build tasks or a bug.",
                            size, getColumnStatsPath());
                }
            } catch (Exception e) {
                logger.error("save column stats file [{}] to HDFS occurred exception",
                        newColStatsPath != null ? newColStatsPath : getColumnStatsPath(), e);
            } finally {
                IOUtils.closeQuietly(out);
            }

        }

        public void delete() {
            try {
                val colStatsPath = new Path(getColumnStatsPath());
                if (fs.exists(colStatsPath)) {
                    fs.delete(colStatsPath, true);
                }
            } catch (IOException e) {
                logger.error("delete column stats file [{}] in HDFS failed, please exec garbage clean",
                        getColumnStatsPath(), e);
            }

        }

        @VisibleForTesting
        public String getColumnStatsPath() {
            val baseDir = KapConfig.wrap(this.config).getReadHdfsWorkingDirectory();
            return baseDir + Paths
                    .get(tableExtDesc.getProject(), ResourceStore.TABLE_EXD_RESOURCE_ROOT, tableExtDesc.getIdentity())
                    .toString();
        }
    }
}
