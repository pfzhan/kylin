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

import io.kyligence.kap.rest.LicenseGatherUtil;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.response.StorageStatisticsResponse;
import io.kyligence.kap.rest.service.LicenseInfoService;
import io.kyligence.kap.rest.service.QuotaService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.parquet.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

@Controller
@Component("kapSystemController")
@RequestMapping(value = "/kap/system")
public class KapSystemController extends BasicController {
    private final static String CODE_UNDEFINED = "400";

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    private QuotaService quotaService;

    @RequestMapping(value = "statistics/storage", produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<StorageStatisticsResponse> getStorageStatistics() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, quotaService.storageStatistics(), "StorageStatistics");
    }

    @RequestMapping(value = "/license", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse listLicense() {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @RequestMapping(value = "/license/file", method = {RequestMethod.POST}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse uploadLisense(@RequestParam("file") MultipartFile uploadfile) throws IOException {

        if (uploadfile.isEmpty()) {
            throw new IllegalArgumentException("please select a file");
        }

        byte[] bytes = uploadfile.getBytes();
        updateLicense(bytes);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    //either content or file is okay
    @RequestMapping(value = "/license/content", method = {RequestMethod.POST}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse uploadLisense(@RequestBody String licenseContent) throws IOException {

        byte[] bytes = null;

        if (!StringUtils.isEmpty(licenseContent)) {
            bytes = licenseContent.getBytes("UTF-8");
        }

        if (bytes == null)
            throw new IllegalArgumentException("license content is empty");

        updateLicense(bytes);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    private void updateLicense(byte[] bytes) throws IOException {

        checkLicense(bytes);

        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File licenseFile = new File(kylinHome, "LICENSE");
        if (licenseFile.exists()) {
            File licenseBackFile = new File(kylinHome, "LICENSE.backup");
            if (licenseBackFile.exists())
                FileUtils.forceDelete(licenseBackFile);
            FileUtils.copyFile(licenseFile, licenseBackFile);
            FileUtils.forceDelete(licenseFile);
        }
        FileUtils.writeByteArrayToFile(licenseFile, bytes);

        //refresh license
        LicenseGatherUtil.gatherLicenseInfo(LicenseGatherUtil.getDefaultLicenseFile(),
                LicenseGatherUtil.getDefaultCommitFile(), LicenseGatherUtil.getDefaultVersionFile(), null);
    }

    private void checkLicense(byte[] bytes) {
    }

    @RequestMapping(value = "/requestLicense", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public void requestLicense(final HttpServletResponse response) throws IOException {

        String info = licenseInfoService.requestLicenseInfo();
        File licenseInfo = File.createTempFile("license", ".info");
        FileUtils.write(licenseInfo, info, Charset.defaultCharset());
        setDownloadResponse(licenseInfo, "license.info", response);
    }

    @RequestMapping(value = "/license/trial", method = {RequestMethod.POST}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public ResponseEntity trialLicense(@RequestBody LicenseRequest licenseRequest) throws IOException {
        if (licenseRequest == null || Strings.isNullOrEmpty(licenseRequest.getEmail())
                || Strings.isNullOrEmpty(licenseRequest.getUserName())
                || Strings.isNullOrEmpty(licenseRequest.getCompany())) {
            return ResponseEntity.badRequest().body(new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, "", "wrong parameter"));
        }
        RemoteLicenseResponse trialLicense = licenseInfoService.getTrialLicense(licenseRequest);
        if (trialLicense == null || !trialLicense.isSuccess()) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new EnvelopeResponse(CODE_UNDEFINED, "", "get license error"));
        }

        return ResponseEntity.ok().body(uploadLisense(trialLicense.getData()));
    }

    private void setDownloadResponse(File downloadFile, String filename, final HttpServletResponse response) {
        KapMessage msg = KapMsgPicker.getMsg();

        try (InputStream fileInputStream = new FileInputStream(downloadFile);
             OutputStream output = response.getOutputStream()) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.setContentLength((int) (downloadFile.length()));
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
            IOUtils.copyLarge(fileInputStream, output);
            output.flush();
        } catch (IOException e) {
            throw new InternalErrorException(msg.getDOWNLOAD_FILE_CREATE_FAIL());
        }
    }
}
