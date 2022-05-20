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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.exception.ModelBrokenException;
import io.kyligence.kap.metadata.project.NProjectManager;
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
            throw new RuntimeException(e);
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
    
    public List<String> getModelNamesByFuzzyName(String fuzzyName) {
        return crud.listAll().stream().filter(model -> StringUtils.containsIgnoreCase(model.getAlias(), fuzzyName)).map(model -> model.getAlias())
                .collect(Collectors.toList());
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

        checkDuplicateModel(model);

        //check model count
        List<NDataModel> allModels = readAllModelsFromSystem(model.getProject());
        NCircuitBreaker.verifyModelCreation(allModels.size());

        copy.setOwner(owner);
        copy.setStorageType(getConfig().getDefaultStorageType());
        model = saveDataModelDesc(copy);
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
        val cached = getDataModelDesc(modelId);
        if (cached == null) {
            throw new KylinException(ErrorCodeServer.MODEL_ID_NOT_EXIST, modelId);
        }
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
        if (!dataModelDesc.getComputedColumnDescs().isEmpty()) {
            List<NDataModel> models = readAllModelsFromSystem(dataModelDesc.getProject());
            dataModelDesc.init(config, project,
                    models.stream().filter(model -> !model.isBroken()).collect(Collectors.toList()));
        } else {
            dataModelDesc.init(config, project, Lists.newArrayList());
        }

        crud.save(dataModelDesc);
        return dataModelDesc;

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

    public static boolean isModelAccessible(NDataModel model) {
        return KylinConfig.getInstanceFromEnv().streamingEnabled() || !model.isStreaming();
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

    private NProjectManager getProjectManager() {
        return NProjectManager.getInstance(config);
    }

    public interface NDataModelUpdater {
        void modify(NDataModel copyForWrite);
    }
}
