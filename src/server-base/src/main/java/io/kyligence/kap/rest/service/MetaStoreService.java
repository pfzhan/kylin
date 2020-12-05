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

import static io.kyligence.kap.common.license.Constants.KE_VERSION;
import static io.kyligence.kap.metadata.model.schema.ImportModelContext.MODEL_REC_PATH;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_EXPORT_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_IMPORT_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_METADATA_FILE_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.persistence.ResourceStore.VERSION_FILE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
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
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.schema.ImportModelContext;
import io.kyligence.kap.metadata.model.schema.ModelImportChecker;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;
import io.kyligence.kap.metadata.query.util.QueryHisStoreUtil;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.ModelImportRequest;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import io.kyligence.kap.rest.response.SimplifiedTablePreviewResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.tool.routine.RoutineTool;
import io.kyligence.kap.tool.util.HashFunction;
import lombok.val;
import lombok.var;

@Component("metaStoreService")
public class MetaStoreService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(MetaStoreService.class);
    private static final String META_ROOT_PATH = "/";

    private static final Pattern MD5_PATTERN = Pattern.compile(".*([a-fA-F\\d]{32})\\.zip");

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    public ModelService modelService;

    @Autowired
    public IndexPlanService indexPlanService;

    public List<ModelPreviewResponse> getPreviewModels(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        return modelService.getDataflowManager(project).listAllDataflows(true).stream().map(df -> {
            if (df.checkBrokenWithRelatedInfo()) {
                NDataModel dataModel = getDataModelManager(project).getDataModelDescWithoutInit(df.getUuid());
                dataModel.setBroken(true);
                return dataModel;
            } else {
                return df.getModel();
            }
        }).map(this::getSimplifiedModelResponse).collect(Collectors.toList());
    }

    private ModelPreviewResponse getSimplifiedModelResponse(NDataModel modelDesc) {
        ModelPreviewResponse modelPreviewResponse = new ModelPreviewResponse();
        modelPreviewResponse.setName(modelDesc.getAlias());
        modelPreviewResponse.setUuid(modelDesc.getUuid());
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                modelDesc.getProject());
        if (modelDesc.isBroken()) {
            modelPreviewResponse.setStatus(ModelStatusToDisplayEnum.BROKEN);
        } else {
            long inconsistentSegmentCount = dfManager.getDataflow(modelDesc.getId())
                    .getSegments(SegmentStatusEnum.WARNING).size();
            ModelStatusToDisplayEnum status = modelService.convertModelStatusToDisplay(modelDesc,
                    modelDesc.getProject(), inconsistentSegmentCount);
            modelPreviewResponse.setStatus(status);

            RawRecManager rawRecManager = RawRecManager.getInstance(modelDesc.getProject());
            val rawRecItems = rawRecManager.displayTopNRecItems(modelDesc.getProject(), modelDesc.getUuid(), 1);
            if (!rawRecItems.isEmpty()) {
                modelPreviewResponse.setHasRecommendation(true);
            }

            NIndexPlanManager indexPlanManager = getIndexPlanManager(modelDesc.getProject());
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelDesc.getUuid());
            if (!indexPlan.getOverrideProps().isEmpty() || (modelDesc.getSegmentConfig() != null
                    && modelDesc.getSegmentConfig().getAutoMergeEnabled() != null
                    && modelDesc.getSegmentConfig().getAutoMergeEnabled())) {
                modelPreviewResponse.setHasOverrideProps(true);
            }

            List<SimplifiedTablePreviewResponse> tables = new ArrayList<>();
            SimplifiedTablePreviewResponse factTable = new SimplifiedTablePreviewResponse(
                    modelDesc.getRootFactTableName(), NDataModel.TableKind.FACT);
            tables.add(factTable);
            List<JoinTableDesc> joinTableDescs = modelDesc.getJoinTables();
            for (JoinTableDesc joinTableDesc : joinTableDescs) {
                SimplifiedTablePreviewResponse lookupTable = new SimplifiedTablePreviewResponse(
                        joinTableDesc.getTable(), joinTableDesc.getKind());
                tables.add(lookupTable);
            }
            modelPreviewResponse.setTables(tables);
        }
        return modelPreviewResponse;
    }

    public ByteArrayOutputStream getCompressedModelMetadata(String project, List<String> modelList,
            boolean exportRecommendations, boolean exportOverProps) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        NDataModelManager modelManager = modelService.getDataModelManager(project);
        NIndexPlanManager indexPlanManager = modelService.getIndexPlanManager(project);
        JdbcRawRecStore jdbcRawRecStore = null;
        if (exportRecommendations) {
            jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {
            ResourceStore oldResourceStore = modelManager.getStore();
            KylinConfig newConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
            ResourceStore newResourceStore = new InMemResourceStore(newConfig);
            ResourceStore.setRS(newConfig, newResourceStore);

            for (String modelId : modelList) {
                NDataModel dataModelDesc = modelManager.getDataModelDesc(modelId);
                if (Objects.isNull(dataModelDesc)) {
                    throw new KylinException(MODEL_NOT_EXIST,
                            String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
                }
                if (dataModelDesc.isBroken()) {
                    throw new KylinException(MODEL_EXPORT_ERROR,
                            String.format(MsgPicker.getMsg().getEXPORT_BROKEN_MODEL(), modelId));
                }

                NDataModel modelDesc = modelManager.copyForWrite(dataModelDesc);

                IndexPlan copyIndexPlan = indexPlanManager.copy(indexPlanManager.getIndexPlan(modelId));

                if (!exportOverProps) {
                    copyIndexPlan.setOverrideProps(Maps.newLinkedHashMap());
                    modelDesc.setSegmentConfig(new SegmentConfig());
                }

                newResourceStore.putResourceWithoutCheck(modelDesc.getResourcePath(),
                        ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(modelDesc)), modelDesc.getLastModified(),
                        modelDesc.getMvcc());

                newResourceStore.putResourceWithoutCheck(copyIndexPlan.getResourcePath(),
                        ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(copyIndexPlan)),
                        copyIndexPlan.getLastModified(), copyIndexPlan.getMvcc());

                // Broken model can't use getAllTables method, will be intercepted in BrokenEntityProxy
                Set<String> tables = modelDesc.getAllTables().stream().map(TableRef::getTableDesc)
                        .map(TableDesc::getResourcePath)
                        .filter(resPath -> !newResourceStore.listResourcesRecursively(META_ROOT_PATH).contains(resPath))
                        .collect(Collectors.toSet());
                tables.forEach(resourcePath -> oldResourceStore.copy(resourcePath, newResourceStore));

                if (exportRecommendations) {
                    List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll(modelDesc.getProject(), modelDesc.getUuid(),
                            modelDesc.getSemanticVersion(), Integer.MAX_VALUE);

                    newResourceStore.putResourceWithoutCheck(String.format(MODEL_REC_PATH, project, modelId),
                            ByteStreams.asByteSource(JsonUtil.writeValueAsIndentBytes(rawRecItems)),
                            System.currentTimeMillis(), -1);
                }
            }
            if (CollectionUtils.isEmpty(newResourceStore.listResourcesRecursively(META_ROOT_PATH))) {
                throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getEXPORT_AT_LEAST_ONE_MODEL());
            }

            // add version file
            String version = System.getProperty(KE_VERSION) == null ? "unknown" : System.getProperty(KE_VERSION);
            newResourceStore.putResourceWithoutCheck(VERSION_FILE, ByteStreams.asByteSource(version.getBytes()),
                    System.currentTimeMillis(), -1);

            oldResourceStore.copy(ResourceStore.METASTORE_UUID_TAG, newResourceStore);
            writeMetadataToZipOutputStream(zipOutputStream, newResourceStore);
        }
        return byteArrayOutputStream;
    }

    private void writeMetadataToZipOutputStream(ZipOutputStream zipOutputStream, ResourceStore resourceStore)
            throws IOException {
        for (String resPath : resourceStore.listResourcesRecursively(META_ROOT_PATH)) {
            zipOutputStream.putNextEntry(new ZipEntry(resPath));
            zipOutputStream.write(resourceStore.getResource(resPath).getByteSource().read());
        }
    }

    private Map<String, RawResource> getRawResourceFromUploadFile(MultipartFile uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try (ZipInputStream zipInputStream = new ZipInputStream(uploadFile.getInputStream())) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.equals(VERSION_FILE)
                        && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }

    private ImportModelContext getImportModelContext(String targetProject, Map<String, RawResource> rawResourceMap,
            ModelImportRequest request) {
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());

        if (request != null) {
            val newModels = request.getModels().stream()
                    .filter(modelImport -> modelImport.getImportType() == ModelImportRequest.ImportType.NEW)
                    .collect(Collectors.toMap(ModelImportRequest.ModelImport::getOriginalName,
                            ModelImportRequest.ModelImport::getTargetName));

            val unImportModels = request.getModels().stream()
                    .filter(modelImport -> modelImport.getImportType() == ModelImportRequest.ImportType.UN_IMPORT)
                    .map(ModelImportRequest.ModelImport::getOriginalName).collect(Collectors.toList());

            return new ImportModelContext(targetProject, srcProject, rawResourceMap, newModels, unImportModels);
        } else {
            return new ImportModelContext(targetProject, srcProject, rawResourceMap);
        }
    }

    public SchemaChangeCheckResult checkModelMetadata(String targetProject, MultipartFile uploadFile,
            ModelImportRequest request) throws IOException {
        String originalFilename = uploadFile.getOriginalFilename();
        Matcher matcher = MD5_PATTERN.matcher(originalFilename);
        boolean valid = false;
        if (matcher.matches()) {
            String signature = matcher.group(1);
            try (InputStream inputStream = uploadFile.getInputStream()) {
                byte[] md5 = HashFunction.MD5.checksum(inputStream);
                valid = StringUtils.equalsIgnoreCase(signature, DatatypeConverter.printHexBinary(md5));
            }
        }

        if (!valid) {
            throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getILLEGAL_MODEL_METADATA_FILE());
        }

        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(uploadFile);

        ImportModelContext context = getImportModelContext(targetProject, rawResourceMap, request);

        return checkModelMetadata(targetProject, context, uploadFile);
    }

    public SchemaChangeCheckResult checkModelMetadata(String targetProject, ImportModelContext context,
            MultipartFile uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(uploadFile);

        checkModelMetadataFile(ResourceStore.getKylinMetaStore(context.getTargetKylinConfig()).getMetadataStore(),
                rawResourceMap.keySet());

        SchemaUtil.SchemaDifference difference = SchemaUtil.diff(targetProject, KylinConfig.getInstanceFromEnv(),
                context.getTargetKylinConfig());

        return ModelImportChecker.check(difference, context);
    }

    private void checkModelMetadataFile(MetadataStore metadataStore, Set<String> rawResourceList) {
        MetadataChecker metadataChecker = new MetadataChecker(metadataStore);
        MetadataChecker.VerifyResult verifyResult = metadataChecker
                .verifyModelMetadata(Lists.newArrayList(rawResourceList));
        if (!verifyResult.isModelMetadataQualified()) {
            throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getMODEL_METADATA_PACKAGE_INVALID());
        }
    }

    private String getModelMetadataProjectName(Set<String> rawResourceList) {
        String anyPath = rawResourceList.stream().filter(
                resourcePath -> resourcePath.indexOf(File.separator) != resourcePath.lastIndexOf(File.separator))
                .findAny().orElse(null);
        if (StringUtils.isBlank(anyPath)) {
            throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getMODEL_METADATA_PACKAGE_INVALID());
        }
        return anyPath.split(File.separator)[1];
    }

    /**
     * 
     * @param nDataModel
     * @param modelImport
     * @param project
     * @param importIndexPlanManager
     */
    private void createNewModel(NDataModel nDataModel, ModelImportRequest.ModelImport modelImport, String project,
            NIndexPlanManager importIndexPlanManager) {
        NDataModelManager dataModelManager = getDataModelManager(project);

        nDataModel.setProject(project);
        nDataModel.setAlias(modelImport.getTargetName());
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setLastModified(System.currentTimeMillis());
        nDataModel.setMvcc(-1);
        dataModelManager.createDataModelDesc(nDataModel, AclPermissionUtil.getCurrentUsername());

        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        var indexPlan = importIndexPlanManager.getIndexPlanByModelAlias(modelImport.getTargetName()).copy();
        indexPlan.setUuid(nDataModel.getUuid());
        indexPlan = indexPlanManager.copy(indexPlan);
        indexPlan.setLastModified(System.currentTimeMillis());
        indexPlan.setMvcc(-1);
        indexPlanManager.createIndexPlan(indexPlan);
        dataflowManager.createDataflow(indexPlan, nDataModel.getOwner(), RealizationStatusEnum.OFFLINE);
    }

    /**
     * 
     * @param project
     * @param nDataModel
     * @param modelImport
     * @param hasModelOverrideProps
     */
    private void updateModel(String project, NDataModel nDataModel, ModelImportRequest.ModelImport modelImport,
            boolean hasModelOverrideProps) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        NDataModel originalDataModel = dataModelManager.getDataModelDescByAlias(modelImport.getOriginalName());
        nDataModel.setProject(project);
        nDataModel.setUuid(originalDataModel.getUuid());
        nDataModel.setLastModified(System.currentTimeMillis());

        // multiple partition column
        if (nDataModel.isMultiPartitionModel()) {
            originalDataModel = modelService.batchUpdateMultiPartition(project, nDataModel.getUuid(),
                    nDataModel.getMultiPartitionDesc().getPartitions().stream()
                            .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList()));

            nDataModel.setMultiPartitionDesc(originalDataModel.getMultiPartitionDesc());
        }

        if (!hasModelOverrideProps) {
            nDataModel.setSegmentConfig(originalDataModel.getSegmentConfig());
        }

        nDataModel.setMvcc(originalDataModel.getMvcc());

        dataModelManager.updateDataModelDesc(nDataModel);
    }

    /**
     * 
     * @param project
     * @param nDataModel
     * @param targetIndexPlan
     * @param hasModelOverrideProps
     */
    private void updateIndexPlan(String project, NDataModel nDataModel, IndexPlan targetIndexPlan,
            boolean hasModelOverrideProps) {
        NIndexPlanManager indexPlanManager = getIndexPlanManager(project);
        indexPlanManager.updateIndexPlan(nDataModel.getUuid(), copyForWrite -> {
            val newRuleBasedCuboid = targetIndexPlan.getRuleBasedIndex();
            newRuleBasedCuboid.setLastModifiedTime(System.currentTimeMillis());
            List<IndexEntity> toBeDeletedIndexes = copyForWrite.getToBeDeletedIndexes();
            toBeDeletedIndexes.clear();
            toBeDeletedIndexes.addAll(targetIndexPlan.getToBeDeletedIndexes());

            if (hasModelOverrideProps) {
                copyForWrite.setOverrideProps(targetIndexPlan.getOverrideProps());
            }

            if (targetIndexPlan.getAggShardByColumns() != null) {
                copyForWrite.setAggShardByColumns(targetIndexPlan.getAggShardByColumns());
            }

            copyForWrite.setRuleBasedIndex(newRuleBasedCuboid, false, true);
        });
    }

    /**
     * 
     * @param project
     * @param modelSchemaChange
     * @param targetIndexPlan
     */
    private void removeIndexes(String project, SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange,
            IndexPlan targetIndexPlan) {
        if (modelSchemaChange != null) {
            val toBeRemovedIndexes = Stream
                    .concat(modelSchemaChange.getReduceItems().stream()
                            .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                            .map(SchemaChangeCheckResult.ChangedItem::getDetail),
                            modelSchemaChange.getUpdateItems().stream()
                                    .filter(schemaUpdate -> schemaUpdate.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                                    .map(SchemaChangeCheckResult.UpdatedItem::getFirstDetail))
                    .map(Long::parseLong).collect(Collectors.toSet());
            if (!toBeRemovedIndexes.isEmpty()) {
                indexPlanService.removeIndexes(project, targetIndexPlan.getId(), toBeRemovedIndexes);
            }
        }
    }

    /**
     * 
     * @param project
     * @param modelSchemaChange
     * @param targetIndexPlan
     */
    private void createTableIndex(String project, SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange,
            IndexPlan targetIndexPlan) {
        if (modelSchemaChange != null) {
            val newIndexes = Stream
                    .concat(modelSchemaChange.getNewItems().stream()
                            .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                            .map(SchemaChangeCheckResult.ChangedItem::getDetail),
                            modelSchemaChange.getUpdateItems().stream()
                                    .filter(schemaUpdate -> schemaUpdate.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                                    .map(SchemaChangeCheckResult.UpdatedItem::getSecondDetail))
                    .map(Long::parseLong).collect(Collectors.toList());

            targetIndexPlan.getWhitelistLayouts().stream().filter(layout -> newIndexes.contains(layout.getId()))
                    .forEach(layout -> indexPlanService.createTableIndex(project, targetIndexPlan.getUuid(), layout,
                            false));
        }
    }

    @Transaction(project = 0, retry = 1)
    public void importModelMetadata(String project, MultipartFile metadataFile, ModelImportRequest request)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(project);

        List<Exception> exceptions = new ArrayList<>();

        val rawResourceMap = getRawResourceFromUploadFile(metadataFile);

        val importModelContext = getImportModelContext(project, rawResourceMap, request);

        val schemaChangeCheckResult = checkModelMetadata(project, importModelContext, metadataFile);

        val importDataModelManager = NDataModelManager.getInstance(importModelContext.getTargetKylinConfig(), project);
        val importIndexPlanManager = NIndexPlanManager.getInstance(importModelContext.getTargetKylinConfig(), project);

        for (ModelImportRequest.ModelImport modelImport : request.getModels()) {
            try {
                validateModelImport(project, modelImport, schemaChangeCheckResult);
                if (modelImport.getImportType() == ModelImportRequest.ImportType.NEW) {
                    var importDataModel = importDataModelManager.getDataModelDescByAlias(modelImport.getTargetName());
                    var nDataModel = importDataModelManager.copyForWrite(importDataModel);

                    createNewModel(nDataModel, modelImport, project, importIndexPlanManager);
                    importRecommendations(project, nDataModel.getUuid(), importDataModel.getUuid(),
                            importModelContext.getTargetKylinConfig());
                } else if (modelImport.getImportType() == ModelImportRequest.ImportType.OVERWRITE) {
                    val importDataModel = importDataModelManager.getDataModelDescByAlias(modelImport.getOriginalName());
                    val nDataModel = importDataModelManager.copyForWrite(importDataModel);

                    // delete index, then remove dimension or measure
                    val targetIndexPlan = importIndexPlanManager.getIndexPlanByModelAlias(modelImport.getOriginalName())
                            .copy();

                    boolean hasModelOverrideProps = (nDataModel.getSegmentConfig() != null
                            && nDataModel.getSegmentConfig().getAutoMergeEnabled() != null
                            && nDataModel.getSegmentConfig().getAutoMergeEnabled())
                            || (!targetIndexPlan.getOverrideProps().isEmpty());

                    val modelSchemaChange = schemaChangeCheckResult.getModels().get(modelImport.getTargetName());

                    removeIndexes(project, modelSchemaChange, targetIndexPlan);
                    updateModel(project, nDataModel, modelImport, hasModelOverrideProps);
                    updateIndexPlan(project, nDataModel, targetIndexPlan, hasModelOverrideProps);
                    createTableIndex(project, modelSchemaChange, targetIndexPlan);

                    importRecommendations(project, nDataModel.getUuid(), importDataModel.getUuid(),
                            importModelContext.getTargetKylinConfig());
                }
            } catch (Exception e) {
                logger.warn("Import model {} exception", modelImport.getOriginalName(), e);
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            String details = exceptions.stream().map(Exception::getMessage).collect(Collectors.joining("\n"));

            throw new KylinException(MODEL_IMPORT_ERROR,
                    String.format("%s\n%s", MsgPicker.getMsg().getIMPORT_MODEL_EXCEPTION(), details), exceptions);
        }
    }

    private void validateModelImport(String project, ModelImportRequest.ModelImport modelImport,
            SchemaChangeCheckResult checkResult) {

        Message msg = MsgPicker.getMsg();

        if (modelImport.getImportType() == ModelImportRequest.ImportType.OVERWRITE) {
            NDataModel dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDescByAlias(modelImport.getOriginalName());

            if (dataModel == null) {
                throw new KylinException(MODEL_IMPORT_ERROR, String.format(msg.getCAN_NOT_OVERWRITE_MODEL(),
                        modelImport.getOriginalName(), modelImport.getImportType()));
            }

            val modelSchemaChange = checkResult.getModels().get(modelImport.getOriginalName());

            if (modelSchemaChange == null || !modelSchemaChange.overwritable()) {
                throw new KylinException(MODEL_IMPORT_ERROR, String.format(msg.getUN_SUITABLE_IMPORT_TYPE(),
                        modelImport.getOriginalName(), modelImport.getImportType()));
            }
        } else if (modelImport.getImportType() == ModelImportRequest.ImportType.NEW) {

            if (!org.apache.commons.lang.StringUtils.containsOnly(modelImport.getTargetName(),
                    ModelService.VALID_NAME_FOR_MODEL)) {
                throw new KylinException(INVALID_MODEL_NAME,
                        String.format(MsgPicker.getMsg().getINVALID_MODEL_NAME(), modelImport.getTargetName()));
            }

            NDataModel dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDescByAlias(modelImport.getTargetName());

            if (dataModel != null) {
                throw new KylinException(INVALID_MODEL_NAME,
                        String.format(msg.getMODEL_ALIAS_DUPLICATED(), modelImport.getTargetName()));
            }

            val modelSchemaChange = checkResult.getModels().get(modelImport.getTargetName());

            if (modelSchemaChange == null || !modelSchemaChange.creatable()) {
                throw new KylinException(MODEL_IMPORT_ERROR, String.format(msg.getUN_SUITABLE_IMPORT_TYPE(),
                        modelImport.getTargetName(), modelImport.getImportType()));
            }

        }
    }

    /**
     * @param project
     * @param targetModelId
     * @param srcModelId
     * @param kylinConfig
     */
    private void importRecommendations(String project, String targetModelId, String srcModelId, KylinConfig kylinConfig)
            throws IOException {
        val manager = RawRecManager.getInstance(project);
        val baseId = manager.getMaxId() + 100;

        Map<Integer, Integer> idChangedMap = new HashMap<>();

        List<RawRecItem> rawRecItems = ImportModelContext.parseRawRecItems(ResourceStore.getKylinMetaStore(kylinConfig),
                project, srcModelId);

        rawRecItems = rawRecItems.stream().peek(rawRecItem -> {
            rawRecItem.setProject(project);
            rawRecItem.setModelID(targetModelId);

            String uniqueFlag = rawRecItem.getUniqueFlag();
            RawRecItem originalRawRecItem = manager.getRawRecItemByUniqueFlag(rawRecItem.getProject(),
                    rawRecItem.getModelID(), uniqueFlag, rawRecItem.getSemanticVersion());
            int newId;
            if (originalRawRecItem != null) {
                newId = originalRawRecItem.getId();
            } else {
                newId = rawRecItem.getId() + baseId;
            }
            idChangedMap.put(-rawRecItem.getId(), -newId);

            rawRecItem.setId(newId);
        }).collect(Collectors.toList());

        ImportModelContext.reorderRecommendations(rawRecItems, idChangedMap);

        manager.batchUpdate(rawRecItems);
    }

    public void cleanupMeta(String project) {
        RoutineTool routineTool = new RoutineTool();
        if (project.equals(UnitOfWork.GLOBAL_UNIT)) {
            routineTool.cleanGlobalMeta();
            QueryHisStoreUtil.cleanQueryHistory();

        } else {
            routineTool.cleanMetaByProject(project);
        }
    }

    public void cleanupStorage(String[] projectsToClean, boolean cleanupStorage) {
        RoutineTool routineTool = new RoutineTool();
        routineTool.setProjects(projectsToClean);
        routineTool.setStorageCleanup(cleanupStorage);
        routineTool.cleanStorage();
    }
}
