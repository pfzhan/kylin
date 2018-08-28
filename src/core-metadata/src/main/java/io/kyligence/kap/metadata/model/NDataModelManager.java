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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;

public class NDataModelManager {
    private static final Logger logger = LoggerFactory.getLogger(NDataModelManager.class);

    public static NDataModelManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataModelManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataModelManager newInstance(KylinConfig conf, String project) {
        try {
            String cls = StringUtil.noBlank(conf.getDataModelManagerImpl(), NDataModelManager.class.getName());
            Class<? extends NDataModelManager> clz = ClassUtil.forName(cls, NDataModelManager.class);
            return clz.getConstructor(KylinConfig.class, String.class).newInstance(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    // name => DataModelDesc
    private CaseInsensitiveStringCache<NDataModel> dataModelDescMap;
    private CachedCrudAssist<NDataModel> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock modelMapLock = new AutoReadWriteLock();

    public NDataModelManager(KylinConfig config, String project) throws IOException {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) throws IOException {
        this.config = cfg;
        this.project = project;
        this.dataModelDescMap = new CaseInsensitiveStringCache<>(config, project, "data_model");
        String resourceRootPath = "/" + project + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataModel>(getStore(), resourceRootPath, NDataModel.class, dataModelDescMap) {
            @Override
            protected NDataModel initEntityAfterReload(NDataModel model, String resourceName) {
                model.setProject(project);
                if (!model.isDraft()) {
                    model.init(config, getAllTablesMap(), listModels(), true);
                }
                return model;
            }
        };

        // touch lower level metadata before registering model listener
        NTableMetadataManager.getInstance(config, project);
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new NDataModelSyncListener(), project, "data_model");
    }

    private class NDataModelSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            //clean up the current project's table desc
            // TODO: Why model changes trigger TableDesc reset?
            NTableMetadataManager.getInstance(config, project).resetProjectSpecificTableDesc();

            try (AutoLock lock = modelMapLock.lockForWrite()) {
                for (String model : getProjectManager().getProject(project).getModels()) {
                    crud.reloadQuietly(model);
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = modelMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    dataModelDescMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }

            broadcaster.notifyProjectSchemaUpdate(project);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // for test mostly
    public Serializer<NDataModel> getDataModelSerializer() {
        return crud.getSerializer();
    }

    public List<NDataModel> getDataModels() {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return new ArrayList<>(dataModelDescMap.values());
        }
    }

    public NDataModel getDataModelDesc(String name) {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return dataModelDescMap.get(name);
        }
    }

    public List<NDataModel> listModels() {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return new ArrayList<>(dataModelDescMap.values());
        }
    }

    // within a project, find models that use the specified table
    public List<String> getModelsUsingTable(TableDesc table) throws IOException {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            List<String> models = new ArrayList<>();
            for (NDataModel modelDesc : listModels()) {
                if (modelDesc.containsTable(table))
                    models.add(modelDesc.getName());
            }
            return models;
        }
    }

    // within a project, find models that use the specified table as root table
    public List<String> getModelsUsingRootTable(TableDesc table) throws IOException {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            List<String> models = new ArrayList<>();
            for (NDataModel modelDesc : listModels()) {
                if (modelDesc.isRootFactTable(table)) {
                    models.add(modelDesc.getName());
                }
            }
            return models;
        }
    }

    public boolean isTableInAnyModel(TableDesc table) {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            for (NDataModel model : listModels()) {
                if (model.containsTable(table))
                    return true;
            }
        }
        return false;
    }

    //    public DataModelDesc reloadDataModel(String modelName) {
    //        try (AutoLock lock = modelMapLock.lockForWrite()) {
    //            return crud.reloadQuietlyAt(resourcePath(modelName));
    //        }
    //    }

    public NDataModel dropModel(NDataModel desc) throws IOException {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            crud.delete(desc);
            // delete model from project
            getProjectManager().removeModel(desc.getName(), desc.getProject());
            return desc;
        }
    }

    public NDataModel createDataModelDesc(NDataModel desc, String owner) throws IOException {
        if (StringUtils.isEmpty(desc.getProject())) {
            desc.setProject(project);
        }
        String name = desc.getName();
        Preconditions.checkArgument(desc.getProject().equals(project), "Model %s belongs to project %s, not %s", name,
                desc.getProject(), project);
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            if (dataModelDescMap.containsKey(name))
                throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");

            NProjectManager prjMgr = getProjectManager();
            ProjectInstance prj = prjMgr.getProject(project);
            if (prj.containsModel(name))
                throw new IllegalStateException("project " + project + " already contains model " + name);

            try {
                // Temporarily register model under project, because we want to
                // update project formally after model is saved.
                prj.getModels().add(name);

                desc.setOwner(owner);
                desc = saveDataModelDesc(desc);

            } finally {
                prj.getModels().remove(name);
            }

            // now that model is saved, update project formally
            prjMgr.addModelToProject(name, project);

            return desc;
        }
    }

    public NDataModel updateDataModelDesc(NDataModel desc) throws IOException {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            String name = desc.getName();
            if (!dataModelDescMap.containsKey(desc.getName())) {
                throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
            }

            return saveDataModelDesc(desc);
        }
    }

    private NDataModel saveDataModelDesc(NDataModel dataModelDesc) throws IOException {
        if (!dataModelDesc.isDraft())
            dataModelDesc.init(config, this.getAllTablesMap(), listModels(), true);

        crud.save(dataModelDesc);

        return dataModelDesc;

    }

    private Map<String, TableDesc> getAllTablesMap() {
        return NTableMetadataManager.getInstance(config, project).getAllTablesMap();
    }

    /**
     * if there is some change need be applied after getting a cubeDesc from front-end, do it here
     * @param dataModel
     */
    private void postProcessKapModel(NDataModel dataModel) {
        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : dataModel.getEffectiveMeasureMap().entrySet()) {
            MeasureDesc measureDesc = measureEntry.getValue();
            if (TopNMeasureType.FUNC_TOP_N.equalsIgnoreCase(measureDesc.getFunction().getExpression())) {
                TopNMeasureType.fixMeasureReturnType(measureDesc);
            }
        }
    }

    private static String resourcePath(String project, String modelName) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(modelName).append(MetadataConstants.FILE_SURFIX).toString();
    }

    /**
     *
     * @param resPath should be exactly like this: /{project_name}/model_desc/{model_name}.json
     * @return {project_name}
     */
    private String getProjectFromPath(String resPath) {
        Preconditions.checkNotNull(resPath);

        String[] parts = resPath.split("/");
        Preconditions.checkArgument(4 == parts.length);
        return parts[1];
    }

    private NProjectManager getProjectManager() {
        return NProjectManager.getInstance(config);
    }
}
