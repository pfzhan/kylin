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
package io.kyligence.kap.license.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_FILE_CONTENT;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_EMAIL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.REMOTE_SERVER_ERROR;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.model.LicenseInfo;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.parquet.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.RefreshVolumeBroadcastEventNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.request.SourceUsageFilter;
import io.kyligence.kap.rest.response.CapacityDetailsResponse;
import io.kyligence.kap.rest.response.LicenseInfoWithDetailsResponse;
import io.kyligence.kap.rest.response.LicenseMonitorInfoResponse;
import io.kyligence.kap.rest.response.ProjectCapacityResponse;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.service.SystemService;
import io.kyligence.kap.license.service.LicenseInfoService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class LicenseController extends NBasicController {

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @Setter
    @Getter
    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Pattern trialPattern = Pattern.compile("\\S[a-zA-Z\\s\\d\\u4e00-\\u9fa5]+\\S");

    @ApiOperation(value = "license", tags = { "MID" })
    @GetMapping(value = "/license")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> listLicense() {
        EnvelopeResponse<LicenseInfo> response = new EnvelopeResponse<>(KylinException.CODE_SUCCESS, new LicenseInfo(), "");
        try {
            LicenseInfoService.licenseReadWriteLock.readLock().lock();
            val info = licenseInfoService.extractLicenseInfo();
            response = new EnvelopeResponse<>(KylinException.CODE_SUCCESS, info, "");
            val warning = licenseInfoService.verifyLicense(info);
            if (warning != null) {
                setResponse(response, LicenseInfoService.CODE_WARNING, warning);
            }
        } catch (KylinException e) {
            setResponse(response, e.getCode(), e.getMessage());
        } finally {
            LicenseInfoService.licenseReadWriteLock.readLock().unlock();
        }
        return response;
    }

    // used for service discovery
    @ApiOperation(value = "backupLicense", tags = { "MID" })
    @PostMapping(value = "/backup")
    @ResponseBody
    public EnvelopeResponse<String> remoteBackupProject(@RequestBody BackupRequest backupRequest) throws Exception {
        systemService.backup(backupRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "uploadLicense", tags = { "MID" })
    @PostMapping(value = "/license/file")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> uploadLicense(@RequestParam("file") MultipartFile uploadfile)
            throws IOException {

        if (uploadfile.isEmpty()) {
            throw new IllegalArgumentException("please select a file");
        }

        byte[] bytes = uploadfile.getBytes();
        licenseInfoService.updateLicense(new String(bytes, Charset.defaultCharset()));

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    //either content or file is okay
    @ApiOperation(value = "updateLicense", tags = { "MID" })
    @PostMapping(value = "/license/content")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> uploadLicense(@RequestBody String licenseContent) throws IOException {

        byte[] bytes = null;

        if (!StringUtils.isEmpty(licenseContent)) {
            bytes = licenseContent.getBytes(StandardCharsets.UTF_8);
        }

        if (ArrayUtils.isEmpty(bytes))
            throw new KylinException(EMPTY_FILE_CONTENT, MsgPicker.getMsg().getContentIsEmpty());

        licenseInfoService.updateLicense(bytes);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @ApiOperation(value = "trialLicense", tags = { "MID" }, notes = "Update Body: product_type")
    @PostMapping(value = "/license/trial")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> trialLicense(@RequestBody LicenseRequest licenseRequest) throws Exception {
        if (licenseRequest == null || Strings.isNullOrEmpty(licenseRequest.getEmail())
                || Strings.isNullOrEmpty(licenseRequest.getUsername())
                || Strings.isNullOrEmpty(licenseRequest.getCompany())) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEmailUsernameCompanyCanNotEmpty());
        }
        if (licenseRequest.getEmail().length() > MAX_NAME_LENGTH
                || licenseRequest.getUsername().length() > MAX_NAME_LENGTH
                || licenseRequest.getCompany().length() > MAX_NAME_LENGTH) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEmailUsernameCompanyIsIllegal());
        }
        if (!licenseInfoService.filterEmail(licenseRequest.getEmail())) {
            throw new KylinException(INVALID_EMAIL, MsgPicker.getMsg().getIllegalEmail());
        }
        if (!trialPattern.matcher(licenseRequest.getCompany()).matches()
                || !trialPattern.matcher(licenseRequest.getUsername()).matches()) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getUsernameCompanyIsIllegal());
        }

        RemoteLicenseResponse trialLicense = licenseInfoService.getTrialLicense(licenseRequest);
        if (trialLicense == null || !trialLicense.isSuccess()) {
            throw new KylinException(REMOTE_SERVER_ERROR, MsgPicker.getMsg().getLicenseError());
        }
        licenseInfoService.updateLicense(trialLicense.getData());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @ApiOperation(value = "get license info", tags = { "MID" })
    @GetMapping(value = "/license/info")
    @ResponseBody
    public void requestLicense(final HttpServletResponse response) throws IOException {
        String info = licenseInfoService.requestLicenseInfo();
        File licenseInfo = File.createTempFile("license", ".info");
        FileUtils.write(licenseInfo, info, Charset.defaultCharset());
        setDownloadResponse(licenseInfo, "license.info", MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
    }

    @ApiOperation(value = "get license monitor info with detail")
    @GetMapping(value = "/capacities")
    @ResponseBody
    public EnvelopeResponse<LicenseInfoWithDetailsResponse> getLicenseMonitorInfoWithDetail(
            @RequestParam(value = "project_names", required = false, defaultValue = "") String[] projectNames,
            @RequestParam(value = "status", required = false, defaultValue = "") String[] statuses,
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "capacity") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        List<String> argProjects = getValidProjects(projectNames, exactMatch);
        List<String> statusList = Arrays.asList(statuses);
        LicenseInfoWithDetailsResponse result;
        if (CollectionUtils.isEmpty(argProjects)) {
            result = new LicenseInfoWithDetailsResponse(0, Lists.newArrayList());
        } else {
            SourceUsageFilter sourceUsageFilter = new SourceUsageFilter(argProjects, statusList, sortBy, reverse);
            result = licenseInfoService.getLicenseMonitorInfoWithDetail(sourceUsageFilter, pageOffset, pageSize);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "get license capacity info", tags = { "MID" })
    @GetMapping(value = "/license/capacity")
    @ResponseBody
    public EnvelopeResponse getLicenseCapacityInfo() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, licenseInfoService.getLicenseCapacityInfo(), "");
    }

    @ApiOperation(value = "get license node info", tags = { "MID" })
    @GetMapping(value = "/license/nodes")
    @ResponseBody
    public EnvelopeResponse getLicenseNodeInfo() {
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, licenseInfoService.getLicenseNodeInfo(), "");
    }

    @ApiOperation(value = "get license monitor info single project", tags = { "MID" })
    @GetMapping(value = "/capacity_info")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoSingleProject(@RequestParam(value = "project") String project,
                                                               @RequestParam(value = "data_range", required = false, defaultValue = "month") String dataRange) {
        Map<Long, Long> projectCapacities = licenseInfoService.getProjectCapacities(project, dataRange);
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, projectCapacities, "");
    }

    @ApiOperation(value = "get license monitor info in project", tags = { "MID" }, notes = "Update Param: page_offset, page_size;")
    @GetMapping(value = "/capacity")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoInProject(@RequestParam(value = "project") String project,
                                                           @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
                                                           @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
                                                           @RequestParam(value = "sort_by", required = false, defaultValue = "capacity") String sortBy,
                                                           @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        aclEvaluate.checkProjectAdminPermission(project);
        SourceUsageFilter sourceUsageFilter = new SourceUsageFilter(Lists.newArrayList(), Lists.newArrayList(), sortBy,
                reverse);
        ProjectCapacityResponse projectCapacityResponse = licenseInfoService.getLicenseMonitorInfoByProject(project,
                sourceUsageFilter);
        if (projectCapacityResponse.getSize() > 0) {
            List<CapacityDetailsResponse> tables = projectCapacityResponse.getTables();
            List<CapacityDetailsResponse> tableCapacityDetailsPaging = PagingUtil.cutPage(tables, pageOffset, pageSize);
            projectCapacityResponse.setTables(tableCapacityDetailsPaging);
        }
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, projectCapacityResponse, "");
    }

    @ApiOperation(value = "get last month/quarter/year license monitor info", tags = { "MID" })
    @GetMapping(value = "/capacity/dashboard")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoHistory(
            @RequestParam(value = "data_range", required = false, defaultValue = "month") String dataRange) {
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, licenseInfoService.getSourceUsageHistory(dataRange), "");
    }

    @ApiOperation(value = "refresh license monitor info in a project", tags = { "MID" })
    @PutMapping(value = "/capacity/refresh")
    @ResponseBody
    public EnvelopeResponse<String> refresh(@RequestParam("project") String project) {
        aclEvaluate.checkIsGlobalAdmin();
        EventBusFactory.getInstance().postAsync(new SourceUsageUpdateNotifier());
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refresh license monitor info in all projects", tags = { "MID" })
    @PutMapping(value = "/capacity/refresh_all")
    @ResponseBody
    public EnvelopeResponse<LicenseMonitorInfoResponse> refreshAll(HttpServletRequest request) {
        aclEvaluate.checkIsGlobalAdmin();
        licenseInfoService.updateSourceUsage();
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, licenseInfoService.getLicenseCapacityInfo(), "");
    }


    //for kc
    @ApiOperation(value = "licenseVolume", tags = { "MID" })
    @PutMapping(value = "/license/volume", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> modifyLicense() {
        licenseInfoService.refreshLicenseVolume();
        EventBusFactory.getInstance().postAsync(new RefreshVolumeBroadcastEventNotifier());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    private void setResponse(EnvelopeResponse response, String errorCode, String message) {
        response.setCode(errorCode);
        response.setMsg(message);
    }


    @VisibleForTesting
    public List<String> getValidProjects(String[] projects, boolean exactMatch) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<ProjectInstance> availableProjects = projectManager.listAllProjects().stream()
                .filter(aclEvaluate::hasProjectAdminPermission).collect(Collectors.toList());

        List<ProjectInstance> projectInstanceList;
        if (0 == projects.length) {
            projectInstanceList = availableProjects;
        } else {
            projectInstanceList = availableProjects.stream().filter(projectInstance -> {
                for (String projectName : projects) {
                    if (exactMatch ? projectInstance.getName().equals(projectName)
                            : projectInstance.getName().toUpperCase(Locale.ROOT)
                            .contains(projectName.toUpperCase(Locale.ROOT))) {
                        return true;
                    }
                }
                return false;
            }).collect(Collectors.toList());
        }

        return projectInstanceList.stream().map(ProjectInstance::getName).collect(Collectors.toList());
    }
}
