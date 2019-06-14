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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NDataModelManager {
    private static final Logger logger = LoggerFactory.getLogger(NDataModelManager.class);

    public static NDataModelManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataModelManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataModelManager newInstance(KylinConfig conf, String project) {
        try {
            String cls = NDataModelManager.class.getName();
            Class<? extends NDataModelManager> clz = ClassUtil.forName(cls, NDataModelManager.class);
            return clz.getConstructor(KylinConfig.class, String.class).newInstance(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<NDataModel> crud;

    public NDataModelManager(KylinConfig config, String project) throws IOException {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) throws IOException {
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT;
        val tableManager = NTableMetadataManager.getInstance(config, project);
        this.crud = new CachedCrudAssist<NDataModel>(getStore(), resourceRootPath, NDataModel.class) {
            @Override
            protected NDataModel initEntityAfterReload(NDataModel model, String resourceName) {

                if (model.getBrokenReason() != NDataModel.BrokenReason.NULL) {
                    throw new ModelBrokenException();
                }

                if (!model.isDraft()) {
                    model.init(config, getAllTablesMap(),
                            listAllValidCache().stream().filter(m -> !m.isBroken()).collect(Collectors.toList()),
                            project);
                }
                postModelRepairEvent(model);
                return model;
            }

            @Override
            protected NDataModel initBrokenEntity(NDataModel entity, String resourceName) {
                NDataModel model = super.initBrokenEntity(entity, resourceName);
                model.setProject(project);
                if (entity != null) {
                    entity.setProject(project);
                    model.setAlias(entity.getAlias());
                    postModelBrokenEvent(entity);
                }
                return model;
            }
        };
    }

    private void postModelBrokenEvent(NDataModel model) {
        if (!model.isModelBroken()) {
            if (UnitOfWork.isAlreadyInTransaction()) {
                UnitOfWork.get().doAfterUnit(() -> SchedulerEventBusFactory
                        .getInstance(KylinConfig.getInstanceFromEnv()).post(new NDataModel.ModelBrokenEvent(model)));
            } else {
                SchedulerEventBusFactory.getInstance(config).post(new NDataModel.ModelBrokenEvent(model));
            }
        }
    }

    private void postModelRepairEvent(NDataModel model) {
        if (model.isModelBroken()) {
            if (UnitOfWork.isAlreadyInTransaction()) {
                UnitOfWork.get().doAfterUnit(
                        () -> SchedulerEventBusFactory.getInstance(KylinConfig.getInstanceFromEnv())
                                .post(new NDataModel.ModelRepairEvent(model)));
            } else {
                SchedulerEventBusFactory.getInstance(config).post(new NDataModel.ModelRepairEvent(model));
            }

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

    public NDataModel getDataModelDesc(String modelId) {
        if (modelId == null) {
            return null;
        }
        return crud.get(modelId);
    }

    public NDataModel getDataModelDescWithoutInit(String modelId) {
        try {
            val resource = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                    .getResource(getDataModelDesc(modelId).getResourcePath());
            val modelDesc = JsonUtil.readValue(resource.getByteSource().read(), NDataModel.class);
            modelDesc.setMvcc(resource.getMvcc());
            modelDesc.setProject(project);
            return modelDesc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isModelBroken(String modelId) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        return dataflow == null || dataflow.checkBrokenWithRelatedInfo();
    }

    public NDataModel getDataModelDescByAlias(String alias) {
        return crud.listAll().stream().filter(model -> Objects.equals(model.getAlias(), alias)).findFirst()
                .orElse(null);
    }

    public NDataModel dropModel(NDataModel desc) {
        crud.delete(desc);
        return desc;
    }

    public Set<String> listAllModelAlias() {
        return crud.listAll().stream().map(NDataModel::getAlias).collect(Collectors.toSet());
    }

    public Set<String> listAllModelIds() {
        return crud.listAll().stream().map(NDataModel::getId).collect(Collectors.toSet());
    }

    public List<NDataModel> listAllModels() {
        return crud.listAll();
    }

    public NDataModel createDataModelDesc(NDataModel desc, String owner) {
        String name = desc.getAlias();
        for (NDataModel model : crud.listAll().stream().filter(model -> !model.isBroken())
                .collect(Collectors.toList())) {
            if (model.getAlias().equals(name)) {
                throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");
            }
        }

        NProjectManager prjMgr = getProjectManager();
        ProjectInstance prj = prjMgr.getProject(project);
        if (prj.containsModel(name))
            throw new IllegalStateException("project " + project + " already contains model " + name);

        desc.setOwner(owner);
        desc = saveDataModelDesc(desc);

        return desc;
    }

    public NDataModel updateDataModel(String model, NDataModelUpdater updater) {
        val cached = getDataModelDesc(model);
        val copy = copyForWrite(cached);
        updater.modify(copy);
        return updateDataModelDesc(copy);
    }

    public NDataModel updateDataModelDesc(NDataModel desc) {
        String name = desc.getUuid();
        if (!crud.contains(desc.getUuid())) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }
        return saveDataModelDesc(desc);
    }

    public NDataModel updateDataBrokenModelDesc(NDataModel desc) {
        return crud.save(desc);
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    private NDataModel saveDataModelDesc(NDataModel dataModelDesc) {
        dataModelDesc.checkSingleIncrementingLoadingTable();
        if (!dataModelDesc.isDraft())
            dataModelDesc.init(config, this.getAllTablesMap(),
                    crud.listAll().stream().filter(model -> !model.isBroken()).collect(Collectors.toList()), project);

        crud.save(dataModelDesc);

        return dataModelDesc;

    }

    private Map<String, TableDesc> getAllTablesMap() {
        return NTableMetadataManager.getInstance(config, project).getAllTablesMap();
    }

    /**
     * @param resPath should be exactly like this: /{project_name}/model_desc/{model_name}.json
     * @return {project_name}
     */
    private String getProjectFromPath(String resPath) {
        Preconditions.checkNotNull(resPath);

        String[] parts = resPath.split("/");
        Preconditions.checkArgument(4 == parts.length);
        return parts[1];
    }

    public NDataModel copyForWrite(NDataModel nDataModel) {
        return crud.copyForWrite(nDataModel);
    }

    public String getModelDisplayName(String modelId) {
        NDataModel dataModelDesc = getDataModelDesc(modelId);
        return dataModelDesc == null ? "NotFoundModel(" + modelId + ")" : dataModelDesc.toString();
    }

    private NProjectManager getProjectManager() {
        return NProjectManager.getInstance(config);
    }

    public interface NDataModelUpdater {
        void modify(NDataModel copyForWrite);
    }

    public void reload(String modelId) {
        val model = getDataModelDesc(modelId);
        if (model != null) {
            crud.reloadAt(model.getResourcePath());
        }
    }
}
