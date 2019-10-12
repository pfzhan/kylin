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

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.parquet.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.service.SystemService;
import lombok.val;

@Controller
@Component("nSystemController")
@RequestMapping(value = "/system")
public class NSystemController extends NBasicController {

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @GetMapping(value = "/license", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listLicense() throws IOException {
        val info = licenseInfoService.extractLicenseInfo();
        val response = new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, info, "");
        try {
            val warning = licenseInfoService.verifyLicense(info);
            if (warning != null) {
                setResponse(response, LicenseInfoService.CODE_WARNING, warning);
            }
        } catch (BadRequestException e) {
            setResponse(response, e.getCode(), e.getMessage());
        }
        return response;

    }

    private void setResponse(EnvelopeResponse response, String errorCode, String message) {
        response.setCode(errorCode);
        response.setMsg(message);
    }

    // used for service discovery
    @PostMapping(value = "/backup", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse remoteBackupProject(@RequestBody BackupRequest backupRequest) throws Exception {
        checkRequiredArg("backupPath", backupRequest.getBackupPath());
        systemService.backup(backupRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/license/file", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse uploadLicense(@RequestParam("file") MultipartFile uploadfile)
            throws IOException, InterruptedException {

        if (uploadfile.isEmpty()) {
            throw new IllegalArgumentException("please select a file");
        }

        byte[] bytes = uploadfile.getBytes();
        licenseInfoService.updateLicense(new String(bytes));

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    //either content or file is okay
    @PostMapping(value = "/license/content", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse uploadLicense(@RequestBody String licenseContent) throws IOException {

        byte[] bytes = null;

        if (!StringUtils.isEmpty(licenseContent)) {
            bytes = licenseContent.getBytes("UTF-8");
        }

        if (bytes == null)
            throw new IllegalArgumentException("license content is empty");

        licenseInfoService.updateLicense(bytes);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @PostMapping(value = "/license/trial", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse trialLicense(@RequestBody LicenseRequest licenseRequest) throws Exception {
        if (licenseRequest == null || Strings.isNullOrEmpty(licenseRequest.getEmail())
                || Strings.isNullOrEmpty(licenseRequest.getUsername())
                || Strings.isNullOrEmpty(licenseRequest.getCompany())) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, "", "wrong parameter");
        }
        RemoteLicenseResponse trialLicense = licenseInfoService.getTrialLicense(licenseRequest);
        if (trialLicense == null || !trialLicense.isSuccess()) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, "", "get license error");
        }
        licenseInfoService.updateLicense(trialLicense.getData());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

}
