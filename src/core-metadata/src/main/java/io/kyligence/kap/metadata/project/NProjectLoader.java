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

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NRealizationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionListenerRegistry;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NProjectLoader {

    private static final Logger logger = LoggerFactory.getLogger(NProjectLoader.class);

    private NProjectManager mgr;

    static {
        TransactionListenerRegistry.register(NProjectLoader::updateCache, project -> NProjectLoader.removeCache());
    }

    private static ThreadLocal<ProjectBundle> cache = new ThreadLocal<>();

    public static void updateCache(@Nullable String project) {
        if (StringUtils.isNotEmpty(project) && !project.startsWith("_")) {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val projectLoader = new NProjectLoader(projectManager);
            if (projectManager.getProject(project) == null) {
                log.debug("project {} not exist", project);
                return;
            }
            removeCache();
            val bundle = projectLoader.load(project);
            log.debug("set project {} cache {}, prev is {}", project, bundle, cache.get());
            cache.set(bundle);
        }
    }

    public static void removeCache() {
        log.debug("clear cache {}", cache.get());
        cache.remove();
    }

    public NProjectLoader(NProjectManager mgr) {
        this.mgr = mgr;
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
                if (belongToFactTable(table, r.getModel()) && (!onlyRewriteMeasure || func.needRewrite())) {
                    result.add(m);
                }
            }
        }
        return result;
    }

    public List<ColumnDesc> listComputedColumns(String project, TableDesc tableDesc) {
        List<ColumnDesc> result = Lists.newArrayList();
        val dfMgr = NDataflowManager.getInstance(mgr.getConfig(), project);
        for (NDataModel r : dfMgr.listUnderliningDataModels()) {
            val computedColumns = ComputedColumnUtil.createComputedColumns(r.getComputedColumnDescs(), tableDesc);
            if (belongToFactTable(tableDesc.getIdentity(), r)) {
                result.addAll(Arrays.asList(computedColumns));
            }
        }

        return result;
    }

    private boolean belongToFactTable(String table, NDataModel model) {
        // measure belong to the fact table
        return model.getRootFactTable().getTableIdentity().equals(table);
    }

    // ============================================================================
    // build the cache
    // ----------------------------------------------------------------------------

    private ProjectBundle load(String project) {
        if (cache.get() != null) {
            return cache.get();
        }
        ProjectBundle projectBundle = new ProjectBundle(project);

        ProjectInstance pi = mgr.getProject(project);

        if (pi == null)
            throw new IllegalArgumentException("Project '" + project + "' does not exist.");

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(mgr.getConfig(), project);

        pi.getTables().forEach(tableName -> {
            TableDesc tableDesc = metaMgr.getTableDesc(tableName);
            if (tableDesc != null) {
                projectBundle.tables.put(tableDesc.getIdentity(), new TableBundle(tableDesc));
            } else {
                logger.warn("Table '{}' defined under project '{}' is not found.", tableName, project);
            }
        });

        NRealizationRegistry registry = NRealizationRegistry.getInstance(mgr.getConfig(), project);
        pi.getRealizationEntries().forEach(entry -> {
            IRealization realization = registry.getRealization(entry.getType(), entry.getRealization());
            if (realization != null) {
                projectBundle.realizations.add(realization);
            } else {
                logger.warn("Realization '{}' defined under project '{}' is not found.", entry, project);
            }
        });

        projectBundle.realizations.forEach(realization -> {
            if (sanityCheck(projectBundle, realization, project)) {
                mapTableToRealization(projectBundle, realization);
                markExposedTablesAndColumns(projectBundle, realization);
            }
        });

        return projectBundle;
    }

    // check all columns reported by realization does exists
    private boolean sanityCheck(ProjectBundle prjCache, IRealization realization, String project) {
        if (realization == null)
            return false;

        Set<TblColRef> allColumns = realization.getAllColumns();

        if (allColumns.isEmpty() && realization.getMeasures().isEmpty()) {
            // empty model is allowed in newten
            logger.trace("Realization '{}' does not report any columns or measures.", realization.getCanonicalName());
            return false;
            // cuboid which only contains measure on (*) should return true
        }

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(mgr.getConfig(), project);
        for (TblColRef col : allColumns) {
            TableDesc table = metaMgr.getTableDesc(col.getTable());
            if (table == null) {
                logger.error("Realization '{}' reports column '{}', but its table is not found by MetadataManager.",
                        realization.getCanonicalName(), col.getCanonicalName());
                return false;
            }

            if (!col.getColumnDesc().isComputedColumn()) {
                ColumnDesc foundCol = table.findColumnByName(col.getName());
                if (!col.getColumnDesc().equals(foundCol)) {
                    logger.error(
                            "Realization '{}' reports column '{}', but it is not equal to '{}' according to MetadataManager.",
                            realization.getCanonicalName(), col.getCanonicalName(), foundCol);
                    return false;
                }
            }

            // auto-define table required by realization for some legacy test case
            if (prjCache.tables.get(table.getIdentity()) == null) {
                prjCache.tables.put(table.getIdentity(), new TableBundle(table));
                logger.warn("Realization '{}' reports column '{}' whose table is not defined in project '{}'.",
                        realization.getCanonicalName(), col.getCanonicalName(), prjCache.project);
            }
        }

        return true;
    }

    private void mapTableToRealization(ProjectBundle prjCache, IRealization realization) {
        final Set<TableRef> allTables = realization.getModel().getAllTables();
        for (TableRef tbl : allTables) {
            TableBundle tableBundle = prjCache.tables.get(tbl.getTableDesc().getIdentity());
            tableBundle.realizations.add(realization);
        }
    }

    private void markExposedTablesAndColumns(ProjectBundle prjCache, IRealization realization) {
        if (!realization.isReady()) {
            return;
        }

        final Set<TableRef> allTables = realization.getModel().getAllTables();
        for (TableRef tbl : allTables) {
            TableBundle tableBundle = prjCache.tables.get(tbl.getTableDesc().getIdentity());
            prjCache.exposedTables.add(tableBundle.tableDesc);
        }

        for (TblColRef col : realization.getAllColumns()) {
            TableBundle tableBundle = prjCache.tables.get(col.getTable());
            Preconditions.checkNotNull(tableBundle);
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
        private TableDesc tableDesc;
        private Set<ColumnDesc> exposedColumns = Sets.newLinkedHashSet();
        private Set<IRealization> realizations = Sets.newLinkedHashSet();

        TableBundle(TableDesc tableDesc) {
            this.tableDesc = tableDesc;
        }
    }

}
