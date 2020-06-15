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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MODEL_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_FORMAT_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_METADATA_FILE_ERROR;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.rest.request.ModelPreviewRequest;
import io.kyligence.kap.rest.response.ModelMetadataCheckResponse;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import io.kyligence.kap.rest.service.MetaStoreService;
import io.kyligence.kap.tool.util.HashFunction;
import io.kyligence.kap.tool.util.ZipFileUtil;

@Controller
@RequestMapping(value = "/api/metastore", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NMetaStoreController extends NBasicController {

    @Autowired
    @Qualifier("metaStoreService")
    private MetaStoreService metaStoreService;

    @GetMapping(value = "/previews/models")
    @ResponseBody
    public EnvelopeResponse<List<ModelPreviewResponse>> previewModels(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        List<ModelPreviewResponse> simplifiedModels = metaStoreService.getPreviewModels(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, simplifiedModels, "");
    }

    @PostMapping(value = "/backup/models", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void exportModelMetadata(@RequestParam(value = "project") String project, ModelPreviewRequest request,
            HttpServletResponse response) throws Exception {
        checkProjectName(project);
        if (CollectionUtils.isEmpty(request.getIds())) {
            throw new KylinException(EMPTY_MODEL_ID, "At least one model should be selected to export!");
        }
        String filename = String.format("%s_model_metadata_%s.zip", project.toLowerCase(),
                new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date()));
        ByteArrayOutputStream byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(project, request.getIds());
        try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray())) {
            setDownloadResponse(byteArrayInputStream, filename, MediaType.APPLICATION_FORM_URLENCODED_VALUE, response);
        }
    }

    @PostMapping(value = "/validation/models")
    @ResponseBody
    public EnvelopeResponse<ModelMetadataCheckResponse> uploadAndCheckModelMetadata(
            @RequestParam(value = "project") String project, @RequestParam("file") MultipartFile uploadFile)
            throws Exception {
        checkProjectName(project);
        checkUploadFile(uploadFile);

        ModelMetadataCheckResponse modelMetadataCheckResponse = metaStoreService.checkModelMetadata(project, uploadFile);
        try(InputStream inputStream = uploadFile.getInputStream()) {
            byte[] md5 = HashFunction.MD5.checksum(inputStream);
            modelMetadataCheckResponse.setSignature(DatatypeConverter.printHexBinary(md5));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelMetadataCheckResponse, "");
    }

    @PostMapping(value = "/models")
    @ResponseBody
    public EnvelopeResponse<String> importModelMetadata(@RequestParam(value = "project") String project,
                                                        @RequestParam(value = "signature") String signature,
                                                        @RequestParam(value = "file") MultipartFile metadataFile,
                                                        ModelPreviewRequest request) throws Exception {
        checkProjectName(project);
        checkUploadFile(metadataFile);
        if (CollectionUtils.isEmpty(request.getIds())) {
            throw new KylinException(EMPTY_MODEL_ID, "At least one model should be selected to import!");
        }
        try(InputStream inputStream = metadataFile.getInputStream()) {
            byte[] md5 = HashFunction.MD5.checksum(inputStream);
            if (!StringUtils.equals(signature, DatatypeConverter.printHexBinary(md5))) {
                throw new KylinException(MODEL_METADATA_FILE_ERROR, "Please verify the metadata file first");
            }
        }

        try {
            metaStoreService.importModelMetadata(project, metadataFile, request.getIds());
        } catch (RuntimeException exception) {
            Throwable rootCause = ExceptionUtils.getRootCause(exception);
            throw new RuntimeException(rootCause);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
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
