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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.rest.response.ModelMetadataCheckResponse.ConflictItem;
import static io.kyligence.kap.rest.response.ModelMetadataCheckResponse.ModelMetadataConflict;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ModelMetadataConflictType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.response.ModelMetadataCheckResponse;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import io.kyligence.kap.rest.response.SimplifiedTablePreviewResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("metaStoreService")
public class MetaStoreService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(MetaStoreService.class);

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    public ModelService modelService;

    public List<ModelPreviewResponse> getPreviewModels(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        return modelService.getDataflowManager(project).listUnderliningDataModels(false)
                .stream()
                .map(this::getSimplifiedModelResponse)
                .collect(Collectors.toList());
    }

    private ModelPreviewResponse getSimplifiedModelResponse(NDataModel modelDesc) {
        ModelPreviewResponse modelPreviewResponse = new ModelPreviewResponse();
        modelPreviewResponse.setName(modelDesc.getAlias());
        modelPreviewResponse.setUuid(modelDesc.getUuid());

        List<SimplifiedTablePreviewResponse> tables = new ArrayList<>();
        SimplifiedTablePreviewResponse factTable = new SimplifiedTablePreviewResponse(modelDesc.getRootFactTableName(),
                NDataModel.TableKind.FACT);
        tables.add(factTable);
        List<JoinTableDesc> joinTableDescs = modelDesc.getJoinTables();
        for (JoinTableDesc joinTableDesc : joinTableDescs) {
            SimplifiedTablePreviewResponse lookupTable = new SimplifiedTablePreviewResponse(joinTableDesc.getTable(),
                    joinTableDesc.getKind());
            tables.add(lookupTable);
        }
        modelPreviewResponse.setTables(tables);

        return modelPreviewResponse;
    }

    public ByteArrayOutputStream getCompressedModelMetadata(String project, List<String> modelList) throws IOException, RuntimeException {
        aclEvaluate.checkProjectWritePermission(project);
        NDataModelManager modelManager = modelService.getDataModelManager(project);
        NIndexPlanManager indexPlanManager = modelService.getIndexPlanManager(project);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try(ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {
            Set<String> resourcePathSet = Sets.newHashSet();
            for (String modelId : modelList) {
                NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
                if (Objects.isNull(modelDesc)) {
                    logger.warn("The model not exist. model id: [{}]", modelId);
                    continue;
                }
                if (modelDesc.isBroken()) {
                    logger.warn("The model is broken, can not export. model id: [{}]", modelId);
                    continue;
                }
                resourcePathSet.add(modelDesc.getResourcePath());
                resourcePathSet.add(indexPlanManager.getIndexPlan(modelId).getResourcePath());
                // Broken model can't use getAllTables method, will be intercepted in BrokenEntityProxy
                resourcePathSet.addAll(modelDesc.getAllTables().stream()
                        .map(TableRef::getTableDesc)
                        .map(TableDesc::getResourcePath)
                        .collect(Collectors.toSet()));
            }
            if (CollectionUtils.isEmpty(resourcePathSet)) {
                throw new KylinException("KE-1005", "Can not export broken model.");
            }
            resourcePathSet.add(ResourceStore.METASTORE_UUID_TAG);
            writeMetadataToZipOutputStream(zipOutputStream, modelManager.getStore(), resourcePathSet);
        }
        return byteArrayOutputStream;
    }

    private void writeMetadataToZipOutputStream(ZipOutputStream zipOutputStream, ResourceStore resourceStore, Set<String> resourcePathSet)
            throws IOException {
        for (val resourcePath : resourcePathSet) {
            zipOutputStream.putNextEntry(new ZipEntry(resourcePath));
            zipOutputStream.write(resourceStore.getResource(resourcePath).getByteSource().read());
        }
    }

    private Map<String, RawResource> getRawResourceFromUploadFile(MultipartFile uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try(ZipInputStream zipInputStream = new ZipInputStream(uploadFile.getInputStream())) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }

    public ModelMetadataCheckResponse checkModelMetadata(String targetProject, MultipartFile uploadFile) throws Exception {
        aclEvaluate.checkProjectWritePermission(targetProject);
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(uploadFile);
        KylinConfig modelConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore modelResourceStore = new InMemResourceStore(modelConfig);
        ResourceStore.setRS(modelConfig, modelResourceStore);
        rawResourceMap.forEach((resPath, raw) -> modelResourceStore.putResourceWithoutCheck(resPath, raw.getByteSource(),
                raw.getTimestamp(), raw.getMvcc()));

        Set<String> resourcePathList = rawResourceMap.keySet();
        checkModelMetadataFile(MetadataStore.createMetadataStore(modelConfig), resourcePathList);
        String srcProjectName = getModelMetadataProjectName(resourcePathList);

        return getModelMetadataCheckResponse(NDataModelManager.getInstance(modelConfig, srcProjectName), targetProject);
    }

    private void checkModelMetadataFile(MetadataStore srcModelMetaStore, Set<String> rawResourceList) {
        MetadataChecker metadataChecker = new MetadataChecker(srcModelMetaStore);
        MetadataChecker.VerifyResult verifyResult = metadataChecker.verifyModelMetadata(Lists.newArrayList(rawResourceList));
        if (!verifyResult.isModelMetadataQualified()) {
            throw new KylinException("KE-1010", MsgPicker.getMsg().getMODEL_METADATA_PACKAGE_INVALID());
        }
    }

    private ModelMetadataCheckResponse getModelMetadataCheckResponse(NDataModelManager srcModelManager, String targetProject) {
        ModelMetadataCheckResponse modelMetadataCheckResponse = new ModelMetadataCheckResponse();
        List<ModelMetadataConflict> conflictList = new ArrayList<>();
        List<ModelPreviewResponse> modelPreviewResponseList = new ArrayList<>();

        for (NDataModel srcModelDesc : srcModelManager.listAllModels()) {
            ModelPreviewResponse modelPreviewResponse = getSimplifiedModelResponse(srcModelDesc);
            modelPreviewResponseList.add(modelPreviewResponse);

            // check DUPLICATE_MODEL_NAME
            ModelMetadataConflict modelNameConflict = getDuplicateModelNameConflict(srcModelDesc, targetProject);
            if (Objects.nonNull(modelNameConflict)) {
                conflictList.add(modelNameConflict);
            }

            // check TABLE_NOT_EXISTED
            List<ModelMetadataConflict> tableNotExistedConflictList = getTableNotExistedConflicts(srcModelDesc,
                    targetProject, modelPreviewResponse);
            if (CollectionUtils.isNotEmpty(tableNotExistedConflictList)) {
                conflictList.addAll(tableNotExistedConflictList);
                continue;
            }

            // check COLUMN_NOT_EXISTED  and check INVALID_COLUMN_DATATYPE
            List<ModelMetadataConflict> columnConflictList = getColumnNotExistedConflict(srcModelDesc, targetProject,
                    modelPreviewResponse);
            if (CollectionUtils.isNotEmpty(columnConflictList)) {
                conflictList.addAll(columnConflictList);
            }
        }
        modelMetadataCheckResponse.setModelMetadataConflictList(conflictList);
        modelMetadataCheckResponse.setModelPreviewResponsesList(modelPreviewResponseList);
        return modelMetadataCheckResponse;
    }

    private List<ModelMetadataConflict> getColumnNotExistedConflict(NDataModel srcModelDesc, String targetProject,
            ModelPreviewResponse srcModelPreviewResponse) {
        List<String> extraTables = getTablesNotExistTargetProject(targetProject, srcModelPreviewResponse);
        List<RootPersistentEntity> dependencies = srcModelDesc.getDependencies();
        List<ModelMetadataConflict> columnNotExistedConflictList = new ArrayList<>();
        // traverse all dependency tables
        for (RootPersistentEntity entity : dependencies) {
            if (!(entity instanceof TableDesc)) {
                continue;
            }
            // no need to check conflict tables
            TableDesc srcTableDesc = (TableDesc) entity;
            if (extraTables.contains(srcTableDesc.getIdentity())) {
                continue;
            }

            NTableMetadataManager tableMetadataManager = modelService.getTableManager(targetProject);
            TableDesc targetTableDesc = tableMetadataManager.getTableDesc(srcTableDesc.getIdentity());

            // check COLUMN_NOT_EXISTED
            List<ModelMetadataConflict> extraColumnsInOneTable = getColumnsNotExistConflicts(srcModelDesc,
                    srcTableDesc, targetTableDesc);
            if (CollectionUtils.isNotEmpty(extraColumnsInOneTable)) {
                columnNotExistedConflictList.addAll(extraColumnsInOneTable);
                continue;
            }

            // check INVALID_COLUMN_DATATYPE
            List<ModelMetadataConflict> invalidColumnsTypeInOneTable = getInvalidColumnDataTypeConflicts(srcModelDesc,
                    srcTableDesc, targetTableDesc);
            if (CollectionUtils.isNotEmpty(invalidColumnsTypeInOneTable)) {
                columnNotExistedConflictList.addAll(invalidColumnsTypeInOneTable);
            }
        }

        return columnNotExistedConflictList;
    }

    private List<ModelMetadataConflict> getColumnsNotExistConflicts(NDataModel srcModelDesc, TableDesc srcTableDesc,
                                                                    TableDesc targetTableDesc) {
        val srcColumnNames = Stream.of(srcTableDesc.getColumns()).map(ColumnDesc::getName).collect(Collectors.toSet());
        val targetColumnNames = Stream.of(targetTableDesc.getColumns()).map(ColumnDesc::getName).collect(Collectors.toSet());
        String element = srcModelDesc.getAlias() + "-" + srcTableDesc.getIdentity();
        return Sets.difference(srcColumnNames, targetColumnNames).stream()
                .map(columnNotExist -> {
                    List<ConflictItem> conflictItem = Lists.newArrayList(new ConflictItem(element, columnNotExist));
                    return new ModelMetadataConflict(ModelMetadataConflictType.COLUMN_NOT_EXISTED, conflictItem);
                })
                .collect(Collectors.toList());
    }

    private List<ModelMetadataConflict> getInvalidColumnDataTypeConflicts(NDataModel srcModelDesc,
                                                                          TableDesc srcTableDesc, TableDesc targetTableDesc) {
        List<ModelMetadataConflict> modelMetadataConflictList = new ArrayList<>();
        // get conflicts column data type in one table
        List<ColumnDesc> srcColumnDescList = Lists.newArrayList(srcTableDesc.getColumns());
        for (ColumnDesc srcColumnDesc : srcColumnDescList) {
            ColumnDesc targetColumnDesc = targetTableDesc.findColumnByName(srcColumnDesc.getName());
            // extra column or correct column
            if (targetColumnDesc == null
                    || StringUtils.equalsIgnoreCase(targetColumnDesc.getDatatype(), srcColumnDesc.getDatatype())) {
                continue;
            }
            String srcElement = String.format("%s-%s-%s", srcModelDesc.getAlias(), srcTableDesc.getIdentity(),
                    srcColumnDesc.getName());
            List<ConflictItem> conflictItems = Lists.newArrayList(new ConflictItem(srcElement, srcColumnDesc.getDatatype()));
            ModelMetadataConflict conflict = new ModelMetadataConflict(
                    ModelMetadataConflictType.INVALID_COLUMN_DATATYPE, conflictItems);
            modelMetadataConflictList.add(conflict);
        }
        return modelMetadataConflictList;
    }

    private ModelMetadataConflict getDuplicateModelNameConflict(NDataModel srcModelDesc, String targetProject) {
        if (Objects.isNull(modelService.getDataModelManager(targetProject).getDataModelDescByAlias(srcModelDesc.getAlias()))) {
            return null;
        }
        List<ConflictItem> conflictItems = Lists.newArrayList(new ConflictItem(srcModelDesc.getAlias(), srcModelDesc.getAlias()));
        return new ModelMetadataConflict(ModelMetadataConflictType.DUPLICATE_MODEL_NAME, conflictItems);
    }

    private List<ModelMetadataConflict> getTableNotExistedConflicts(NDataModel srcModelDesc, String targetProject,
            ModelPreviewResponse srcModelPreviewResponse) {
        List<String> conflictTables = getTablesNotExistTargetProject(targetProject, srcModelPreviewResponse);

        return conflictTables.stream().map(tableNotExist -> {
            List<ConflictItem> conflictItem = Lists.newArrayList(new ConflictItem(srcModelDesc.getAlias(), tableNotExist));
            return new ModelMetadataConflict(ModelMetadataConflictType.TABLE_NOT_EXISTED, conflictItem);
        }).collect(Collectors.toList());
    }

    private List<String> getTablesNotExistTargetProject(String targetProject,
            ModelPreviewResponse srcModelPreviewResponse) {
        List<String> targetTableNameList = modelService.getTableManager(targetProject).listAllTables().stream()
                .map(TableDesc::getIdentity).collect(Collectors.toList());

        return srcModelPreviewResponse.getTables().stream().filter(
                simplifiedTablePreviewResponse -> !targetTableNameList.contains(simplifiedTablePreviewResponse.getName().toUpperCase()))
                .map(SimplifiedTablePreviewResponse::getName).collect(Collectors.toList());
    }

    private String getModelMetadataProjectName(Set<String> rawResourceList) {
        String anyPath = rawResourceList.stream()
                .filter(resourcePath -> !resourcePath.startsWith(ResourceStore.METASTORE_UUID_TAG)).findAny()
                .orElse(null);
        if (StringUtils.isBlank(anyPath)) {
            throw new KylinException("KE-1010", MsgPicker.getMsg().getMODEL_METADATA_PACKAGE_INVALID());
        }
        return anyPath.split(File.separator)[1];
    }

    @Transaction(project = 0, retry = 1)
    public void importModelMetadata(String project, MultipartFile metadataFile, List<String> importModelIds) throws IOException, RuntimeException {
        aclEvaluate.checkProjectWritePermission(project);
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(metadataFile);
        String srcProjectName = getModelMetadataProjectName(rawResourceMap.keySet());
        NDataModelManager targetDataModelManager = getDataModelManager(project);
        NIndexPlanManager targetIndexPlanManager = getIndexPlanManager(project);
        NDataflowManager targetDataFlowManager = getDataflowManager(project);
        for (String modelId : importModelIds) {
            String modelResPath = NDataModel.concatResourcePath(modelId, srcProjectName);
            RawResource modelRaw = rawResourceMap.get(modelResPath);
            NDataModel srcModel = JsonUtil.readValue(modelRaw.getByteSource().read(), NDataModel.class);
            String newUuid = UUID.randomUUID().toString();
            srcModel.setUuid(newUuid);
            srcModel.setProject(project);
            srcModel.setLastModified(0L);
            srcModel.setMvcc(-1);
            try {
                targetDataModelManager.createDataModelDesc(srcModel, AclPermissionUtil.getCurrentUsername());
            } catch (RuntimeException e) {
                thrownImportException(e, srcModel.getAlias());
            }

            String indexPlanResPath = IndexPlan.concatResourcePath(modelId, srcProjectName);
            RawResource indexPlanRaw = rawResourceMap.get(indexPlanResPath);
            IndexPlan srcIndexPlan = JsonUtil.readValue(indexPlanRaw.getByteSource().read(), IndexPlan.class);
            srcIndexPlan.setUuid(newUuid);
            srcIndexPlan.setProject(project);
            srcIndexPlan.setLastModified(0L);
            srcIndexPlan.setMvcc(-1);
            targetIndexPlanManager.createIndexPlan(srcIndexPlan);

            NDataflow dataFlow = targetDataFlowManager.createDataflow(srcIndexPlan, AclPermissionUtil.getCurrentUsername());
            targetDataFlowManager.updateDataflow(dataFlow.getId(), copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.WARNING));
        }
    }

    private void thrownImportException(RuntimeException exception, String srcModelName) {
        String message = exception.getMessage();
        if (exception instanceof LookupTableException) {
            message = String.format(MsgPicker.getMsg().getFACT_TABLE_USED_AS_LOOK_UP_TABLE(), srcModelName);
            throw new KylinException("KE-1005", message, ResponseCode.CODE_UNDEFINED);
        }
        if (exception instanceof BadModelException) {
            BadModelException badModelException = (BadModelException) exception;
            switch (badModelException.getCauseType()) {
                case SAME_EXPR_DIFF_NAME:
                    message = String.format(MsgPicker.getMsg().getCOMPUTED_COLUMN_EXPRESSION_ALREADY_DEFINED(),
                        srcModelName, badModelException.getBadCC(), badModelException.getConflictingModel(), badModelException.getAdvise());
                    break;
                case SAME_NAME_DIFF_EXPR:
                    message = String.format(MsgPicker.getMsg().getCOMPUTED_COLUMN_NAME_ALREADY_DEFINED(),
                            srcModelName, badModelException.getBadCC(), badModelException.getConflictingModel(), badModelException.getAdvise());
                    break;
                default:
                    throw new KylinException("KE-1005", message, ResponseCode.CODE_UNDEFINED, exception);
            }
            throw new KylinException("KE-1021", message, ResponseCode.CODE_UNDEFINED);
        }
        throw new KylinException("KE-1005", message, ResponseCode.CODE_UNDEFINED, exception);
    }
}
