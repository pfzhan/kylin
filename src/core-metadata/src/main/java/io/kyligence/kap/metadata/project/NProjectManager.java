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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class NProjectManager implements IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NProjectManager.class);

    public static NProjectManager getInstance(KylinConfig config) {
        return config.getManager(NProjectManager.class);
    }

    // called by reflection
    static NProjectManager newInstance(KylinConfig config) {
        return new NProjectManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private NProjectLoader projectLoader;

    private CachedCrudAssist<ProjectInstance> crud;

    public NProjectManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NProjectManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.projectLoader = new NProjectLoader(this);
        crud = new CachedCrudAssist<ProjectInstance>(getStore(), ResourceStore.PROJECT_ROOT, ProjectInstance.class) {
            @Override
            protected ProjectInstance initEntityAfterReload(ProjectInstance entity, String projectName) {
                entity.setName(projectName);
                entity.init(config);
                return entity;
            }
        };
    }

    public List<ProjectInstance> listAllProjects() {
        return crud.listAll();
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    public ProjectInstance getProject(String projectName) {
        return crud.listAll().stream()
                .filter(projectInstance -> projectInstance.getName().equalsIgnoreCase(projectName)).findAny()
                .orElse(null);
    }

    public ProjectInstance createProject(String projectName, String owner, String description,
            LinkedHashMap<String, String> overrideProps, MaintainModelType maintainModelType) {
        logger.info("Creating project " + projectName);

        ProjectInstance currentProject = getProject(projectName);
        if (currentProject == null) {
            //circuit breaker
            NCircuitBreaker.verifyProjectCreation(listAllProjects().size());

            currentProject = ProjectInstance.create(projectName, owner, description, overrideProps, maintainModelType);
            currentProject.initConfig(config);
        } else {
            throw new IllegalStateException("The project named " + projectName + "already exists");
        }
        checkOverrideProps(currentProject);
        return save(currentProject);
    }

    private void checkOverrideProps(ProjectInstance prj) {
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

    public ProjectInstance dropProject(String projectName) {
        if (StringUtils.isEmpty(projectName))
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
        crud.delete(projectName);
        return projectInstance;
    }

    // update project itself
    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc,
            LinkedHashMap<String, String> overrideProps) {
        Preconditions.checkArgument(project.getName().equals(newName));
        return updateProject(newName, copyForWrite -> {
            copyForWrite.setName(newName);
            copyForWrite.setDescription(newDesc);
            copyForWrite.setOverrideKylinProps(overrideProps);
            if (copyForWrite.getUuid() == null)
                copyForWrite.updateRandomUuid();
        });
    }

    public ProjectInstance updateProject(ProjectInstance project) {
        if (getProject(project.getName()) == null) {
            throw new IllegalArgumentException("Project '" + project.getName() + "' does not exist!");
        }
        return save(project);
    }

    public ProjectInstance copyForWrite(ProjectInstance projectInstance) {
        Preconditions.checkNotNull(projectInstance);
        return crud.copyForWrite(projectInstance);
    }

    private ProjectInstance save(ProjectInstance prj) {
        Preconditions.checkArgument(prj != null);
        if (getStore().getConfig().isCheckCopyOnWrite()) {
            if (prj.isCachedAndShared()) {
                throw new IllegalStateException(
                        "Copy-on-write violation! The updating entity " + prj + " is a shared object in "
                                + ProjectInstance.class.getSimpleName() + " cache, which should not be.");
            }
        }
        crud.save(prj);
        return prj;
    }

    public String getDefaultDatabase(String project) {
        return getProject(project).getDefaultDatabase();
    }

    public Set<IRealization> listAllRealizations(String project) {
        return projectLoader.listAllRealizations(project);
    }

    public Set<IRealization> getRealizationsByTable(String project, String tableName) {
        return projectLoader.getRealizationsByTable(project, tableName.toUpperCase(Locale.ROOT));
    }

    public List<NDataModel> listHealthyModels(String project) {
        return listAllRealizations(project).stream().map(IRealization::getModel).collect(Collectors.toList());
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        return projectLoader.listEffectiveRewriteMeasures(project, factTable.toUpperCase(Locale.ROOT), true);
    }

    KylinConfig getConfig() {
        return config;
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public void forceDropProject(String project) {
        if (StringUtils.isEmpty(project))
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(project);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + project + " does not exist");
        }
        val paths = Optional.ofNullable(getStore().listResourcesRecursively(project)).orElse(Sets.newTreeSet());
        for (val path : paths) {
            getStore().deleteResource(path);
        }
        crud.delete(project);
    }

    public interface NProjectUpdater {
        void modify(ProjectInstance copyForWrite);
    }

    public ProjectInstance updateProject(String project, NProjectUpdater updater) {
        val cached = getProject(project);
        val copy = copyForWrite(cached);
        updater.modify(copy);
        return updateProject(copy);
    }
}
