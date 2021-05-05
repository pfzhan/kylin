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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NProjectLoader {

    private static final Logger logger = LoggerFactory.getLogger(NProjectLoader.class);

    private NProjectManager mgr;

    private static ThreadLocal<ProjectBundle> cache = new ThreadLocal<>();

    public static void updateCache(@Nullable String project) {
        if (StringUtils.isNotEmpty(project) && !project.startsWith("_")) {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val projectLoader = new NProjectLoader(projectManager);
            if (projectManager.getProject(project) == null) {
                log.debug("project {} not exist", project);
                return;
            }
            val bundle = projectLoader.load(project);
            log.trace("set project {} cache {}, prev is {}", project, bundle, cache.get());
            cache.set(bundle);
        }
    }

    public static void removeCache() {
        log.trace("clear cache {}", cache.get());
        cache.remove();
    }

    public NProjectLoader(NProjectManager mgr) {
        this.mgr = mgr;
    }

    public Set<IRealization> listAllRealizations(String project) {
        ProjectBundle prjCache = load(project);
        return Collections.unmodifiableSet(
                prjCache.realizationsByTable.values().stream().flatMap(Set::stream).collect(Collectors.toSet()));
    }

    public Set<IRealization> getRealizationsByTable(String project, String table) {
        Set<IRealization> realizationsByTable = load(project).realizationsByTable.get(table);
        if (realizationsByTable == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(realizationsByTable);
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String table, boolean onlyRewriteMeasure) {
        Set<IRealization> realizations = getRealizationsByTable(project, table);
        List<MeasureDesc> result = Lists.newArrayList();
        for (IRealization r : realizations) {
            if (!r.isReady())
                continue;
            NDataModel model = r.getModel();
            for (MeasureDesc m : r.getMeasures()) {
                FunctionDesc func = m.getFunction();
                if (belongToFactTable(table, model) && (!onlyRewriteMeasure || func.needRewrite())) {
                    result.add(m);
                }
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
        Map<String, TableDesc> projectAllTables = metaMgr.getAllTablesMap();
        NRealizationRegistry registry = NRealizationRegistry.getInstance(mgr.getConfig(), project);
        pi.getRealizationEntries().forEach(entry -> {
            IRealization realization = registry.getRealization(entry.getType(), entry.getRealization());
            if (realization == null) {
                logger.warn("Realization '{}' defined under project '{}' is not found or it's broken.", entry, project);
                return;
            }

            if (sanityCheck(realization, projectAllTables)) {
                mapTableToRealization(projectBundle, realization);
            }
        });

        return projectBundle;
    }

    // check all columns reported by realization does exists
    private boolean sanityCheck(IRealization realization, Map<String, TableDesc> projectAllTables) {
        if (realization == null)
            return false;

        Set<TblColRef> allColumns = realization.getAllColumns();

        if (allColumns.isEmpty() && realization.getMeasures().isEmpty()) {
            // empty model is allowed in newten
            logger.trace("Realization '{}' does not report any columns or measures.", realization.getCanonicalName());
            return false;
            // cuboid which only contains measure on (*) should return true
        }

        for (TblColRef col : allColumns) {
            TableDesc table = projectAllTables.get(col.getTable());
            if (table == null) {
                logger.error("Realization '{}' reports column '{}', but its table is not found by MetadataManager.",
                        realization.getCanonicalName(), col.getCanonicalName());
                return false;
            }

            if (!col.getColumnDesc().isComputedColumn()) {
                ColumnDesc foundCol = table.findColumnByName(col.getOriginalName());
                if (!col.getColumnDesc().equals(foundCol)) {
                    logger.error(
                            "Realization '{}' reports column '{}', but it is not equal to '{}' according to MetadataManager.",
                            realization.getCanonicalName(), col.getCanonicalName(), foundCol);
                    return false;
                }
            }
        }

        return true;
    }

    private void mapTableToRealization(ProjectBundle prjCache, IRealization realization) {
        final Set<TableRef> allTables = realization.getModel().getAllTables();
        for (TableRef tbl : allTables) {
            prjCache.realizationsByTable.computeIfAbsent(tbl.getTableIdentity(), value -> Sets.newHashSet());
            prjCache.realizationsByTable.get(tbl.getTableIdentity()).add(realization);
        }
    }

    private static class ProjectBundle {
        private String project;
        private Map<String, Set<IRealization>> realizationsByTable = Maps.newHashMap();

        ProjectBundle(String project) {
            this.project = project;
        }
    }

}
