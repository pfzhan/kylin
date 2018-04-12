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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.badquery.NBadQueryHistoryManager;

public class NProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(NProjectManager.class);

    public static NProjectManager getInstance(KylinConfig config) {
        return config.getManager(NProjectManager.class);
    }

    // called by reflection
    static NProjectManager newInstance(KylinConfig config) throws IOException {
        return new NProjectManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private NProjectL2Cache l2Cache;

    // project name => ProjrectInstance
    private CaseInsensitiveStringCache<ProjectInstance> projectMap;
    private CachedCrudAssist<ProjectInstance> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock prjMapLock = new AutoReadWriteLock();

    public NProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing ProjectManager with metadata url " + config);
        this.config = config;
        this.projectMap = new CaseInsensitiveStringCache<ProjectInstance>(config, "project");
        this.l2Cache = new NProjectL2Cache(this);
        this.crud = new CachedCrudAssist<ProjectInstance>(getStore(), ResourceStore.PROJECT_RESOURCE_ROOT,
                ProjectInstance.class, projectMap) {
            @Override
            protected ProjectInstance initEntityAfterReload(ProjectInstance prj, String resourceName) {
                prj.init();
                return prj;
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new ProjectSyncListener(), "project");
    }

    private class ProjectSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String project = cacheKey;

            if (event == Event.DROP) {
                removeProjectLocal(project);
                return;
            }

            reloadProjectQuietly(project);
            broadcaster.notifyProjectSchemaUpdate(project);
            broadcaster.notifyProjectDataUpdate(project);
        }
    }

    public void clearL2Cache() {
        l2Cache.clear();
    }

    public ProjectInstance reloadProjectQuietly(String project) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            ProjectInstance prj = crud.reloadQuietly(project);
            clearL2Cache();
            return prj;
        }
    }

    public List<ProjectInstance> listAllProjects() {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            return new ArrayList<ProjectInstance>(projectMap.values());
        }
    }

    public ProjectInstance getProject(String projectName) {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            return projectMap.get(projectName);
        }
    }

    public ProjectInstance getPrjByUuid(String uuid) {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            for (ProjectInstance prj : projectMap.values()) {
                if (uuid.equals(prj.getUuid()))
                    return prj;
            }
            return null;
        }
    }

    public ProjectInstance createProject(String projectName, String owner, String description,
            LinkedHashMap<String, String> overrideProps) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            logger.info("Creating project " + projectName);

            ProjectInstance currentProject = getProject(projectName);
            if (currentProject == null) {
                currentProject = ProjectInstance.create(projectName, owner, description, overrideProps, null, null);
            } else {
                throw new IllegalStateException("The project named " + projectName + "already exists");
            }
            checkOverrideProps(currentProject);

            return save(currentProject);
        }
    }

    private void checkOverrideProps(ProjectInstance prj) throws IOException {
        LinkedHashMap<String, String> overrideProps = prj.getOverrideKylinProps();

        if (overrideProps != null) {
            Iterator<Map.Entry<String, String>> iterator = overrideProps.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();

                if (StringUtils.isAnyBlank(entry.getKey(), entry.getValue())) {
                    throw new IllegalStateException("Property key/value must not be blank");
                }
            }
        }
    }

    public ProjectInstance dropProject(String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            if (projectName == null)
                throw new IllegalArgumentException("Project name not given");

            ProjectInstance projectInstance = getProject(projectName);

            if (projectInstance == null) {
                throw new IllegalStateException("The project named " + projectName + " does not exist");
            }

            if (projectInstance.getRealizationCount(null) != 0) {
                throw new IllegalStateException("The project named " + projectName
                        + " can not be deleted because there's still realizations in it. Delete them first.");
            }

            logger.info("Dropping project '" + projectInstance.getName() + "'");

            crud.delete(projectInstance);
            NBadQueryHistoryManager.getInstance(config, projectName).removeBadQueryHistory();

            clearL2Cache();
            return projectInstance;
        }
    }

    // update project itself
    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc,
            LinkedHashMap<String, String> overrideProps) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            Preconditions.checkArgument(project.getName().equals(newName));
            project.setName(newName);
            project.setDescription(newDesc);
            project.setOverrideKylinProps(overrideProps);

            if (project.getUuid() == null)
                project.updateRandomUuid();

            return save(project);
        }
    }

    public void removeProjectLocal(String proj) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            projectMap.removeLocal(proj);
            clearL2Cache();
        }
    }

    public ProjectInstance addModelToProject(String modelName, String newProjectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            removeModel(modelName, newProjectName);

            ProjectInstance prj = getProject(newProjectName);
            if (prj == null) {
                throw new IllegalArgumentException("Project " + newProjectName + " does not exist.");
            }
            prj.addModel(modelName);

            return save(prj);
        }
    }

    public void removeModel(String modelName, String project) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            ProjectInstance projectInstance = getProject(project);
            projectInstance.removeModel(modelName);
            save(projectInstance);
        }
    }

    public void removeModelFromProjects(String modelName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            for (ProjectInstance projectInstance : findProjectsByModel(modelName)) {
                projectInstance.removeModel(modelName);
                save(projectInstance);
            }
        }
    }

    public ProjectInstance moveRealizationToProject(String realizationType, String realizationName,
            String newProjectName, String owner) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            removeRealizationsFromProjects(realizationType, realizationName);
            return addRealizationToProject(realizationType, realizationName, newProjectName, owner);
        }
    }

    private ProjectInstance addRealizationToProject(String realizationType, String realizationName, String project,
            String user) throws IOException {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalArgumentException("Project name should not be empty.");
        }
        ProjectInstance newProject = getProject(project);
        if (newProject == null) {
            newProject = this.createProject(project, user,
                    "This is a project automatically added when adding realization " + realizationName + "("
                            + realizationType + ")",
                    null);
        }
        newProject.addRealizationEntry(realizationType, realizationName);
        save(newProject);

        return newProject;
    }

    public void removeRealizationsFromProjects(String realizationType, String realizationName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            for (ProjectInstance projectInstance : findProjects(realizationType, realizationName)) {
                projectInstance.removeRealization(realizationType, realizationName);
                save(projectInstance);
            }
        }
    }

    public ProjectInstance addTableDescToProject(String[] tableIdentities, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            NTableMetadataManager metaMgr = getTableManager(projectName);
            ProjectInstance projectInstance = getProject(projectName);
            for (String tableId : tableIdentities) {
                TableDesc table = metaMgr.getTableDesc(tableId);
                if (table == null) {
                    throw new IllegalStateException("Cannot find table '" + table + "' in metadata manager");
                }
                projectInstance.addTable(table.getIdentity());
            }

            return save(projectInstance);
        }
    }

    public void removeTableDescFromProject(String tableIdentities, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            NTableMetadataManager metaMgr = getTableManager(projectName);
            ProjectInstance projectInstance = getProject(projectName);
            TableDesc table = metaMgr.getTableDesc(tableIdentities);
            if (table == null) {
                throw new IllegalStateException("Cannot find table '" + table + "' in metadata manager");
            }

            projectInstance.removeTable(table.getIdentity());
            save(projectInstance);
        }
    }

    public ProjectInstance addExtFilterToProject(String[] filters, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            NTableMetadataManager metaMgr = getTableManager(projectName);
            ProjectInstance projectInstance = getProject(projectName);
            for (String filterName : filters) {
                ExternalFilterDesc extFilter = metaMgr.getExtFilterDesc(filterName);
                if (extFilter == null) {
                    throw new IllegalStateException(
                            "Cannot find external filter '" + filterName + "' in metadata manager");
                }
                projectInstance.addExtFilter(filterName);
            }

            return save(projectInstance);
        }
    }

    public void removeExtFilterFromProject(String filterName, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            NTableMetadataManager metaMgr = getTableManager(projectName);
            ProjectInstance projectInstance = getProject(projectName);
            ExternalFilterDesc filter = metaMgr.getExtFilterDesc(filterName);
            if (filter == null) {
                throw new IllegalStateException("Cannot find external filter '" + filterName + "' in metadata manager");
            }

            projectInstance.removeExtFilter(filterName);
            save(projectInstance);
        }
    }

    private ProjectInstance save(ProjectInstance prj) throws IOException {
        crud.save(prj);
        clearL2Cache();
        return prj;
    }

    public ProjectInstance getProjectOfModel(String model) {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            for (ProjectInstance prj : projectMap.values()) {
                if (prj.getModels().contains(model))
                    return prj;
            }
            throw new IllegalStateException("No project found for model " + model);
        }
    }

    public List<ProjectInstance> findProjects(String realizationType, String realizationName) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> result = Lists.newArrayList();
            for (ProjectInstance prj : projectMap.values()) {
                for (RealizationEntry entry : prj.getRealizationEntries()) {
                    if (entry.getType().equals(realizationType) && entry.getRealization().equals(realizationName)) {
                        result.add(prj);
                        break;
                    }
                }
            }
            return result;
        }
    }

    public List<ProjectInstance> findProjectsByModel(String modelName) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
            for (ProjectInstance projectInstance : projectMap.values()) {
                if (projectInstance.containsModel(modelName)) {
                    projects.add(projectInstance);
                }
            }
            return projects;
        }
    }

    public List<ProjectInstance> findProjectsByTable(String tableIdentity) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
            for (ProjectInstance projectInstance : projectMap.values()) {
                if (projectInstance.containsTable(tableIdentity)) {
                    projects.add(projectInstance);
                }
            }
            return projects;
        }
    }

    public Map<String, ExternalFilterDesc> listExternalFilterDescs(String project) {
        return l2Cache.listExternalFilterDesc(project);
    }

    public List<TableDesc> listDefinedTables(String project) {
        return l2Cache.listDefinedTables(project);
    }

    private Collection<TableDesc> listExposedTablesByRealizations(String project) {
        return l2Cache.listExposedTables(project);
    }

    public Collection<TableDesc> listExposedTables(String project, boolean exposeMore) {
        if (exposeMore) {
            return listDefinedTables(project);
        } else {
            return listExposedTablesByRealizations(project);
        }
    }

    public List<ColumnDesc> listExposedColumns(String project, TableDesc tableDesc, boolean exposeMore) {
        Set<ColumnDesc> exposedColumns = l2Cache.listExposedColumns(project, tableDesc.getIdentity());

        if (exposeMore) {
            Set<ColumnDesc> dedup = Sets.newHashSet(tableDesc.getColumns());
            dedup.addAll(exposedColumns);
            return Lists.newArrayList(dedup);
        } else {
            return Lists.newArrayList(exposedColumns);
        }
    }

    public Set<IRealization> listAllRealizations(String project) {
        return l2Cache.listAllRealizations(project);
    }

    public Set<IRealization> getRealizationsByTable(String project, String tableName) {
        return l2Cache.getRealizationsByTable(project, tableName.toUpperCase());
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(project, factTable.toUpperCase(), true);
    }

    public List<MeasureDesc> listEffectiveMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(project, factTable.toUpperCase(), false);
    }

    KylinConfig getConfig() {
        return config;
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    NTableMetadataManager getTableManager(String project) {
        return NTableMetadataManager.getInstance(config, project);
    }

}
