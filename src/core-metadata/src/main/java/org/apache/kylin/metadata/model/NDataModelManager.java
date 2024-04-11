/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.lock.MemoryLockUtils;
import org.apache.kylin.common.persistence.lock.ModuleLockEnum;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.exception.ModelBrokenException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NDataModelManager {

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
            throw new IllegalStateException("Failed to init DataModelManager from " + conf, e);
        }
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<NDataModel> crud;

    public NDataModelManager(KylinConfig config, String project) {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) {
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataModel>(getStore(), resourceRootPath, NDataModel.class) {
            @Override
            protected NDataModel initEntityAfterReload(NDataModel model, String resourceName) {

                if (model.getBrokenReason() != NDataModel.BrokenReason.NULL) {
                    throw new ModelBrokenException();
                }

                if (!model.getComputedColumnDescs().isEmpty()) {
                    model.init(config, project,
                            listAllValidCache().stream().filter(m -> !m.isBroken()).collect(Collectors.toList()));
                } else {
                    model.init(config, project, Lists.newArrayList());
                }
                postModelRepairEvent(model);
                return model;
            }

            @Override
            protected NDataModel initBrokenEntity(NDataModel entity, String resourceName) {
                NDataModel model = super.initBrokenEntity(entity, resourceName);
                model.setProject(project);
                model.setConfig(config);

                if (entity != null) {
                    model.setHandledAfterBroken(entity.isHandledAfterBroken());
                    model.setAlias(entity.getAlias());
                    model.setRootFactTableName(entity.getRootFactTableName());
                    model.setJoinTables(entity.getJoinTables());
                    model.setDependencies(model.calcDependencies());
                    model.setModelType(entity.getModelType());
                    model.setLastModified(entity.getLastModified());

                    postModelBrokenEvent(entity);
                }

                return model;
            }
        };
    }

    private void postModelBrokenEvent(NDataModel model) {

        if (!model.isHandledAfterBroken()) {
            if (UnitOfWork.isAlreadyInTransaction()) {
                UnitOfWork.get().doAfterUnit(() -> EventBusFactory.getInstance()
                        .postAsync(new NDataModel.ModelBrokenEvent(project, model.getUuid())));
            } else {
                EventBusFactory.getInstance().postAsync(new NDataModel.ModelBrokenEvent(project, model.getUuid()));
            }
        }
    }

    private void postModelRepairEvent(NDataModel model) {
        if (model.isHandledAfterBroken()) {
            if (UnitOfWork.isAlreadyInTransaction()) {
                UnitOfWork.get().doAfterUnit(() -> EventBusFactory.getInstance()
                        .postAsync(new NDataModel.ModelRepairEvent(project, model.getUuid())));
            } else {
                EventBusFactory.getInstance().postAsync(new NDataModel.ModelRepairEvent(project, model.getUuid()));
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
        if (StringUtils.isEmpty(modelId)) {
            return null;
        }
        return crud.get(modelId);
    }

    public NDataModel getDataModelDescWithoutInit(String modelId) {
        try {
            val resource = ResourceStore.getKylinMetaStore(config)
                    .getResource(getDataModelDesc(modelId).getResourcePath());
            val modelDesc = JsonUtil.readValue(resource.getByteSource().read(), NDataModel.class);
            modelDesc.setMvcc(resource.getMvcc());
            modelDesc.setProject(project);
            return modelDesc;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean isModelBroken(String modelId) {
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val dataflow = dataflowManager.getDataflow(modelId);
        return dataflow == null || dataflow.checkBrokenWithRelatedInfo();
    }

    public NDataModel getDataModelDescByAlias(String alias) {
        return crud.listAll().stream().filter(model -> model.getAlias().equalsIgnoreCase(alias)).findFirst()
                .orElse(null);
    }

    public NDataModel dropModel(NDataModel desc) {
        crud.delete(desc);
        return desc;
    }

    public NDataModel dropModel(String id) {
        val model = getDataModelDesc(id);
        if (model == null) {
            return null;
        }
        crud.delete(model);
        return model;
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

    private List<NDataModel> readAllModelsFromSystem(String project) {
        KylinConfig systemKylinConfig = KylinConfig.readSystemKylinConfig();
        Preconditions.checkNotNull(systemKylinConfig, "System KylinConfig is null.");
        NDataModelManager instance = NDataModelManager.getInstance(systemKylinConfig, project);
        return instance.listAllModels();
    }

    public void checkDuplicateModel(NDataModel model) {
        List<NDataModel> allModels = readAllModelsFromSystem(model.getProject());
        for (NDataModel existingModel : allModels) {
            if (existingModel.getAlias().equalsIgnoreCase(model.getAlias())
                    || existingModel.getUuid().equals(model.getUuid())) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getDuplicateModelName(), model.getAlias()));
            }
        }
    }

    public NDataModel createDataModelDesc(NDataModel model, String owner) {
        NDataModel copy = copyForWrite(model);

        MemoryLockUtils.manuallyLockModule(project, ModuleLockEnum.MODEL, getStore());
        checkDuplicateModel(model);

        //check model count
        List<NDataModel> allModels = readAllModelsFromSystem(model.getProject());
        NCircuitBreaker.verifyModelCreation(allModels.size());

        copy.setOwner(owner);
        copy.setStorageType(getConfig().getDefaultStorageType());
        model = saveModel(copy);
        return model;
    }

    public Set<Long> addPartitionsIfAbsent(NDataModel model, List<String[]> partitionValues) {
        Preconditions.checkState(model.isMultiPartitionModel());
        Set<Long> partitionIds = Sets.newHashSet();
        NDataModel copy = copyForWrite(model);
        AtomicLong maxPartitionId = new AtomicLong(copy.getMultiPartitionDesc().getMaxPartitionID());

        partitionValues.forEach(value -> {
            MultiPartitionDesc.PartitionInfo partition = copy.getMultiPartitionDesc().getPartitionByValue(value);
            if (partition != null) {
                partitionIds.add(partition.getId());
            } else {
                value = wrapValue(value);
                if (value.length != 0) {
                    MultiPartitionDesc.PartitionInfo toAddPartition = new MultiPartitionDesc.PartitionInfo(
                            maxPartitionId.incrementAndGet(), wrapValue(value));
                    copy.getMultiPartitionDesc().getPartitions().add(toAddPartition);
                    partitionIds.add(toAddPartition.getId());
                }
            }
        });
        copy.getMultiPartitionDesc().setMaxPartitionID(maxPartitionId.get());
        crud.save(copy);
        return partitionIds;
    }

    private String[] wrapValue(String[] value) {
        return Arrays.stream(value).map(String::trim).filter(StringUtils::isNotBlank).toArray(String[]::new);
    }

    public NDataModel updateDataModel(String modelId, NDataModelUpdater updater) {
        NDataModel cached = getDataModelDesc(modelId);
        if (cached == null) {
            throw new KylinException(ErrorCodeServer.MODEL_ID_NOT_EXIST, modelId);
        }
        if (cached.isBroken()) {
            cached = getDataModelDescWithoutInit(modelId);
        }
        val copy = copyForWrite(cached);
        updater.modify(copy);
        return saveModel(copy);
    }

    /**
     * @deprecated Use updateDataModel(String modelId, NDataModelUpdater updater) instead.
     */
    @Deprecated
    public NDataModel updateDataModelDesc(NDataModel desc) {
        String name = desc.getUuid();
        if (!crud.contains(desc.getUuid())) {
            throw new IllegalArgumentException("Model '" + name + "' does not exist.");
        }
        desc.init(config, project, getCCRelatedModels(desc));
        return updateDataModel(desc.getUuid(), desc::copyPropertiesTo);
    }

    public NDataModel updateDataBrokenModelDesc(NDataModel desc) {
        return crud.save(desc);
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    private NDataModel saveModel(NDataModel model) {
        model.checkSingleIncrementingLoadingTable();
        model.init(config, project, getCCRelatedModels(model));
        crud.save(model);
        return model;

    }

    public NDataModel copyForWrite(NDataModel nDataModel) {
        return crud.copyForWrite(nDataModel);
    }

    /**
     * copyBySerialization will not use cache
     */
    public NDataModel copyBySerialization(NDataModel dataModel) {
        return crud.copyBySerialization(dataModel);
    }

    public String getModelDisplayName(String modelId) {
        NDataModel dataModelDesc = getDataModelDesc(modelId);
        return dataModelDesc == null ? "NotFoundModel(" + modelId + ")" : dataModelDesc.toString();
    }

    public static Map<String, TableDesc> getRelatedTables(NDataModel dataModel, String project) {
        Map<String, TableDesc> tableMapping = Maps.newHashMap();
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(dataModel.getConfig(), project);
        if (dataModel.getRootFactTableName() != null) {
            tableMapping.put(dataModel.getRootFactTableName(),
                    tableManager.getTableDesc(dataModel.getRootFactTableName()));
        }
        if (dataModel.getJoinTables() != null) {
            dataModel.getJoinTables().forEach(joinTable -> tableMapping.put(joinTable.getTable(),
                    tableManager.getTableDesc(joinTable.getTable())));
        }
        return tableMapping;
    }

    /**
     * 1. model initialization only considers models with computed-columns
     * 2. reloading table may update more than 2 models
     * 3. reloading table may set model to status of broken and delete computed-columns, other than add computed-columns
     * 4. in order to avoid broken models produced by the current process, get these models in UnitOfWork again
     */
    public List<NDataModel> getCCRelatedModels(NDataModel model) {
        if (model.getComputedColumnDescs().isEmpty()) {
            return Lists.newArrayList();
        }
        NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
        return manager.listAllDataflows(true).stream() //
                .filter(df -> df.checkBrokenWithRelatedInfo() || !df.getModel().getComputedColumnDescs().isEmpty()) //
                .map(df -> NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .getDataflow(df.getId()))
                .filter(df -> !df.checkBrokenWithRelatedInfo()) //
                .map(NDataflow::getModel).collect(Collectors.toList());
    }

    public interface NDataModelUpdater {
        void modify(NDataModel copyForWrite);
    }
}
