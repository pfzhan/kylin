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

package io.kyligence.kap.secondstorage.management;

import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_JSON})
public class SecondStorageEndpoint extends NBasicController {

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("secondStorageService")
    private SecondStorageService secondStorageService;

    public SecondStorageEndpoint setSecondStorageService(SecondStorageService secondStorageService) {
        this.secondStorageService = secondStorageService;
        return this;
    }

    @ApiOperation(value = "loadSegments")
    @PostMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse>
    loadStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("model", request.getModel());
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        return internalLoadIntoStorage(request);
    }

    @ApiOperation(value = "enableModel")
    @PostMapping(value = "/model/state")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> enableStorage(@RequestBody ModelEnableRequest modelEnableRequest) {
        checkProjectName(modelEnableRequest.getProject());
        checkRequiredArg("model", modelEnableRequest.getModel());
        val jobInfo = secondStorageService.changeModelSecondStorageState(modelEnableRequest.getProject(),
                modelEnableRequest.getModel(), modelEnableRequest.isEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "enableProject")
    @PostMapping(value = "/project/state")
    public EnvelopeResponse<JobInfoResponse> enableProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        checkProjectName(projectEnableRequest.getProject());
        val jobInfo = secondStorageService.changeProjectSecondStorageState(projectEnableRequest.getProject(),
                projectEnableRequest.getNewNodes(),
                projectEnableRequest.isEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "disableProjectStorageValidation")
    @PostMapping(value="/project/state/validation")
    public EnvelopeResponse<List<String>> validateProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        checkProjectName(projectEnableRequest.getProject());
        List<String> models = secondStorageService.getAllSecondStorageModel(projectEnableRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, models, "");
    }

    @ApiOperation(value = "listSecondStorageNodes")
    @GetMapping(value = "/nodes")
    @ResponseBody
    public EnvelopeResponse<List<NodeData>> listNodes() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, secondStorageService.listAvailableNodes(), "");
    }

    private EnvelopeResponse<JobInfoResponse> internalLoadIntoStorage(StorageRequest request) {
        String[] segIds = modelService.convertSegmentIdWithName(request.getModel(), request.getProject(),
                request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));

        if (ArrayUtils.isEmpty(segIds)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
        }

        if (!request.storageTypeSupported()) {
            throw new KylinException(INVALID_PARAMETER, "");
        }

        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(modelService.exportSegmentToSecondStorage(request.getProject(), request.getModel(), segIds));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }
}
