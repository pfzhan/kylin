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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_IMPORT_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_FORMAT_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_BROKEN;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NMetaStoreController;
import io.kyligence.kap.rest.request.ModelPreviewRequest;
import io.kyligence.kap.rest.request.OpenModelPreviewRequest;
import io.kyligence.kap.rest.response.ModelMetadataCheckResponse;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import io.kyligence.kap.rest.response.OpenModelMetadataImportResponse;
import io.kyligence.kap.rest.service.MetaStoreService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.tool.util.ZipFileUtil;
import lombok.val;

@Controller
@RequestMapping(value = "/api/metastore", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenMetaStoreController extends NBasicController {

    @Autowired
    @Qualifier("metaStoreService")
    private MetaStoreService metaStoreService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private NMetaStoreController metaStoreController;

    @GetMapping(value = "/previews/models")
    @ResponseBody
    public EnvelopeResponse<List<ModelPreviewResponse>> previewModels(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return metaStoreController.previewModels(project);
    }

    @PostMapping(value = "/backup/models")
    @ResponseBody
    public EnvelopeResponse<String> exportModelMetadata(@RequestParam(value = "project") String project, @RequestBody OpenModelPreviewRequest request,
                                    HttpServletResponse response) throws Exception {
        checkProjectName(project);
        checkExportModelsValid(project, request);
        ModelPreviewRequest modelPreviewRequest = convertToModelPreviewRequest(project, request);
        metaStoreController.exportModelMetadata(project, modelPreviewRequest, response);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/import/models")
    @ResponseBody
    public EnvelopeResponse<OpenModelMetadataImportResponse> importModelMetadata(@RequestParam(value = "project") String project,
                                                        @RequestParam(value = "file") MultipartFile metadataFile,
                                                        OpenModelPreviewRequest request) throws Exception {
        checkProjectName(project);
        checkUploadFile(metadataFile);
        checkRequestModelNamesNotEmpty(request);

        ModelMetadataCheckResponse metadataCheckResponse = metaStoreService.checkModelMetadata(project, metadataFile, request.getNames());

        OpenModelMetadataImportResponse importResponse = new OpenModelMetadataImportResponse();
        importResponse.setFileName(metadataFile.getOriginalFilename());
        importResponse.setModelMetadataConflictList(metadataCheckResponse.getModelMetadataConflictList());
        importResponse.setModelPreviewResponsesList(metadataCheckResponse.getModelPreviewResponsesList());

        // has conflict, import failed.
        if (CollectionUtils.isNotEmpty(metadataCheckResponse.getModelMetadataConflictList())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, importResponse,
                    String.format("%s:%s", FAILED_IMPORT_MODEL.toErrorCode().getLocalizedString(), MsgPicker.getMsg().getMODEL_METADATA_CHECK_FAILED()));
        }

        metaStoreService.importModelMetadataWithModelNames(project, metadataFile, request.getNames());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, importResponse, "");
    }

    private ModelPreviewRequest convertToModelPreviewRequest(String project, OpenModelPreviewRequest request) {
        // have checked model names exist
        NDataModelManager modelManager = modelService.getDataModelManager(project);
        List<String> modelIds = request.getNames()
                .stream()
                .map(name -> modelManager.getDataModelDescByAlias(name).getUuid())
                .collect(Collectors.toList());
        ModelPreviewRequest modelPreviewRequest = new ModelPreviewRequest();
        modelPreviewRequest.setIds(modelIds);
        return modelPreviewRequest;
    }

    private void checkRequestModelNamesNotEmpty(OpenModelPreviewRequest request) {
        List<String> modelNames = request.getNames();
        if (CollectionUtils.isEmpty(modelNames)) {
            throw new KylinException(EMPTY_MODEL_NAME, "The names cannot be empty.");
        }
    }

    private void checkExportModelsValid(String project, OpenModelPreviewRequest request) {
        checkRequestModelNamesNotEmpty(request);
        NDataModelManager modelManager = modelService.getDataModelManager(project);
        for (String modelName : request.getNames()) {
            val modelDesc = modelManager.getDataModelDescByAlias(modelName);
            if (Objects.isNull(modelDesc)) {
                throw new KylinException(MODEL_NOT_EXIST, String.format("The model is not exist. Model name: [%s].", modelName));
            }
            if (modelDesc.isBroken()) {
                throw new KylinException(MODEL_BROKEN, String.format("Broken model cannot be exported. Model name: [%s].", modelName));
            }
        }
    }

    private void checkUploadFile(MultipartFile uploadFile) {
        if (Objects.isNull(uploadFile) || uploadFile.isEmpty()) {
            throw new KylinException(FILE_NOT_EXIST, "please select a file");
        }
        if (!ZipFileUtil.validateZipFilename(uploadFile.getOriginalFilename())) {
            throw new KylinException(FILE_FORMAT_ERROR, "upload file must end with .zip");
        }
    }
}
