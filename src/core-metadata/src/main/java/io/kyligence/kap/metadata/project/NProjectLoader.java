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

package io.kyligence.kap.metadata.project;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NRealizationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.val;

/**
 * This is a second level cache that is built on top of first level cached objects,
 * including Realization, TableDesc, ColumnDesc etc, to speed up query time metadata lookup.
 * <p/>
 * On any object update, the L2 cache simply gets wiped out because it's cheap to rebuild.
 */
class NProjectLoader {

    private static final Logger logger = LoggerFactory.getLogger(NProjectLoader.class);

    private NProjectManager mgr;

    NProjectLoader(NProjectManager mgr) {
        this.mgr = mgr;
    }

    public void clear() {
    }

    public Map<String, ExternalFilterDesc> listExternalFilterDesc(String project) {
        ProjectBundle prjCache = load(project);
        return Collections.unmodifiableMap(prjCache.extFilters);
    }

    public List<TableDesc> listDefinedTables(String project) {
        ProjectBundle prjCache = load(project);
        List<TableDesc> result = Lists.newArrayListWithCapacity(prjCache.tables.size());
        for (TableBundle tableBundle : prjCache.tables.values()) {
            result.add(tableBundle.tableDesc);
        }
        return result;
    }

    public Set<TableDesc> listExposedTables(String project) {
        ProjectBundle prjCache = load(project);
        return Collections.unmodifiableSet(prjCache.exposedTables);
    }

    public Set<ColumnDesc> listExposedColumns(String project, String table) {
        TableBundle tableBundle = load(project).tables.get(table);
        if (tableBundle == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(tableBundle.exposedColumns);
    }

    public Set<IRealization> listAllRealizations(String project) {
        ProjectBundle prjCache = load(project);
        return Collections.unmodifiableSet(prjCache.realizations);
    }

    public Set<IRealization> getRealizationsByTable(String project, String table) {
        TableBundle tableBundle = load(project).tables.get(table);
        if (tableBundle == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(tableBundle.realizations);
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String table, boolean onlyRewriteMeasure) {
        Set<IRealization> realizations = getRealizationsByTable(project, table);
        List<MeasureDesc> result = Lists.newArrayList();
        for (IRealization r : realizations) {
            if (!r.isReady())
                continue;

            for (MeasureDesc m : r.getMeasures()) {
                FunctionDesc func = m.getFunction();
                if (belongToTable(table, r.getModel())) {
                    if (!onlyRewriteMeasure || func.needRewrite()) {
                        result.add(m);
                    }
                }
            }
        }
        return result;
    }

    public List<ColumnDesc> listComputedColumns(String project, TableDesc tableDesc) {
        List<ColumnDesc> result = Lists.newArrayList();
        val modelMgr = NDataModelManager.getInstance(mgr.getConfig(), project);
        for (NDataModel r : modelMgr.listModels()) {
            val computedColumns = ComputedColumnUtil.createComputedColumns(r.getComputedColumnDescs(), tableDesc);
            if (belongToTable(tableDesc.getIdentity(), r)) {
                result.addAll(Arrays.asList(computedColumns));
            }
        }

        return result;
    }

    private boolean belongToTable(String table, NDataModel model) {
        // measure belong to the fact table
        return model.getRootFactTable().getTableIdentity().equals(table);
    }

    // ============================================================================
    // build the cache
    // ----------------------------------------------------------------------------

    private ProjectBundle load(String project) {
        //        logger.debug("Loading L2 project cache for " + project);
        ProjectBundle projectBundle = new ProjectBundle(project);

        ProjectInstance pi = mgr.getProject(project);

        if (pi == null)
            throw new IllegalArgumentException("Project '" + project + "' does not exist;");

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(mgr.getConfig(), project);

        for (String tableName : pi.getTables()) {
            TableDesc tableDesc = metaMgr.getTableDesc(tableName);
            if (tableDesc != null) {
                projectBundle.tables.put(tableDesc.getIdentity(), new TableBundle(tableDesc));
            } else {
                logger.warn("Table '" + tableName + "' defined under project '" + project + "' is not found");
            }
        }

        for (String extFilterName : pi.getExtFilters()) {
            ExternalFilterDesc filterDesc = metaMgr.getExtFilterDesc(extFilterName);
            if (filterDesc != null) {
                projectBundle.extFilters.put(extFilterName, filterDesc);
            } else {
                logger.warn(
                        "External Filter '" + extFilterName + "' defined under project '" + project + "' is not found");
            }
        }

        NRealizationRegistry registry = NRealizationRegistry.getInstance(mgr.getConfig(), project);
        for (RealizationEntry entry : pi.getRealizationEntries()) {
            IRealization realization = registry.getRealization(entry.getType(), entry.getRealization());
            if (realization != null) {
                projectBundle.realizations.add(realization);
            } else {
                logger.warn("Realization '" + entry + "' defined under project '" + project + "' is not found");
            }

        }

        for (IRealization realization : projectBundle.realizations) {
            if (sanityCheck(projectBundle, realization, project)) {
                mapTableToRealization(projectBundle, realization);
                markExposedTablesAndColumns(projectBundle, realization);
            }
        }

        return projectBundle;
    }

    // check all columns reported by realization does exists
    private boolean sanityCheck(ProjectBundle prjCache, IRealization realization, String project) {
        if (realization == null)
            return false;

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(mgr.getConfig(), project);

        Set<TblColRef> allColumns = realization.getAllColumns();
        if (allColumns == null || allColumns.isEmpty()) {
            logger.error("Realization '" + realization.getCanonicalName() + "' does not report any columns");
            return false;
        }

        for (TblColRef col : allColumns) {
            TableDesc table = metaMgr.getTableDesc(col.getTable());
            if (table == null) {
                logger.error("Realization '" + realization.getCanonicalName() + "' reports column '"
                        + col.getCanonicalName() + "', but its table is not found by MetadataManager");
                return false;
            }

            if (!col.getColumnDesc().isComputedColumn()) {
                ColumnDesc foundCol = table.findColumnByName(col.getName());
                if (col.getColumnDesc().equals(foundCol) == false) {
                    logger.error("Realization '" + realization.getCanonicalName() + "' reports column '"
                            + col.getCanonicalName() + "', but it is not equal to '" + foundCol
                            + "' according to MetadataManager");
                    return false;
                }
            } else {
                //computed column may not exit here
            }

            // auto-define table required by realization for some legacy test case
            if (prjCache.tables.get(table.getIdentity()) == null) {
                prjCache.tables.put(table.getIdentity(), new TableBundle(table));
                logger.warn(
                        "Realization '" + realization.getCanonicalName() + "' reports column '" + col.getCanonicalName()
                                + "' whose table is not defined in project '" + prjCache.project + "'");
            }
        }

        return true;
    }

    private void mapTableToRealization(ProjectBundle prjCache, IRealization realization) {
        for (TblColRef col : realization.getAllColumns()) {
            TableBundle tableBundle = prjCache.tables.get(col.getTable());
            tableBundle.realizations.add(realization);
        }
    }

    private void markExposedTablesAndColumns(ProjectBundle prjCache, IRealization realization) {
        if (!realization.isReady()) {
            return;
        }

        for (TblColRef col : realization.getAllColumns()) {
            TableBundle tableBundle = prjCache.tables.get(col.getTable());
            prjCache.exposedTables.add(tableBundle.tableDesc);
            tableBundle.exposed = true;
            tableBundle.exposedColumns.add(col.getColumnDesc());
        }
    }

    private static class ProjectBundle {
        private String project;
        private Map<String, TableBundle> tables = Maps.newHashMap();
        private Set<TableDesc> exposedTables = Sets.newHashSet();
        private Set<IRealization> realizations = Sets.newHashSet();
        private Map<String, ExternalFilterDesc> extFilters = Maps.newHashMap();

        ProjectBundle(String project) {
            this.project = project;
        }
    }

    private static class TableBundle {
        private boolean exposed = false;
        private TableDesc tableDesc;
        private Set<ColumnDesc> exposedColumns = Sets.newLinkedHashSet();
        private Set<IRealization> realizations = Sets.newLinkedHashSet();

        TableBundle(TableDesc tableDesc) {
            this.tableDesc = tableDesc;
        }
    }

}
