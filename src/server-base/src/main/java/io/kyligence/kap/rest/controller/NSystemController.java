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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.rest.request.DiagPackageRequest;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.request.MaintenanceModeRequest;
import io.kyligence.kap.rest.request.SourceUsageFilter;
import io.kyligence.kap.rest.response.CapacityDetailsResponse;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.rest.response.LicenseInfoWithDetailsResponse;
import io.kyligence.kap.rest.response.MaintenanceModeResponse;
import io.kyligence.kap.rest.response.ProjectCapacityResponse;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import io.kyligence.kap.rest.response.ServersResponse;
import io.kyligence.kap.rest.service.MaintenanceModeService;
import io.kyligence.kap.rest.service.SystemService;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.model.LicenseInfo;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.parquet.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_FILE_CONTENT;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_EMAIL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.REMOTE_SERVER_ERROR;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NSystemController extends NBasicController {

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @Autowired
    @Qualifier("maintenanceModeService")
    private MaintenanceModeService maintenanceModeService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Pattern trialPattern = Pattern.compile("\\S[a-zA-Z\\s\\d\\u4e00-\\u9fa5]+\\S");

    @VisibleForTesting
    public void setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
    }

    @VisibleForTesting
    public AclEvaluate getAclEvaluate() {
        return this.aclEvaluate;
    }

    @GetMapping(value = "/license")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> listLicense() {
        val info = licenseInfoService.extractLicenseInfo();
        val response = new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, info, "");
        try {
            val warning = licenseInfoService.verifyLicense(info);
            if (warning != null) {
                setResponse(response, LicenseInfoService.CODE_WARNING, warning);
            }
        } catch (KylinException e) {
            setResponse(response, e.getCode(), e.getMessage());
        }
        return response;
    }

    private void setResponse(EnvelopeResponse response, String errorCode, String message) {
        response.setCode(errorCode);
        response.setMsg(message);
    }

    // used for service discovery
    @PostMapping(value = "/backup")
    @ResponseBody
    public EnvelopeResponse<String> remoteBackupProject(@RequestBody BackupRequest backupRequest) throws Exception {
        systemService.backup(backupRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/license/file")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> uploadLicense(@RequestParam("file") MultipartFile uploadfile)
            throws IOException {

        if (uploadfile.isEmpty()) {
            throw new IllegalArgumentException("please select a file");
        }

        byte[] bytes = uploadfile.getBytes();
        licenseInfoService.updateLicense(new String(bytes));

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    //either content or file is okay
    @PostMapping(value = "/license/content")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> uploadLicense(@RequestBody String licenseContent) throws IOException {

        byte[] bytes = null;

        if (!StringUtils.isEmpty(licenseContent)) {
            bytes = licenseContent.getBytes("UTF-8");
        }

        if (ArrayUtils.isEmpty(bytes))
            throw new KylinException(EMPTY_FILE_CONTENT, MsgPicker.getMsg().getCONTENT_IS_EMPTY());

        licenseInfoService.updateLicense(bytes);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @ApiOperation(value = "trialLicense (update)", notes = "Update Body: product_type")
    @PostMapping(value = "/license/trial")
    @ResponseBody
    public EnvelopeResponse<LicenseInfo> trialLicense(@RequestBody LicenseRequest licenseRequest) throws Exception {
        if (licenseRequest == null || Strings.isNullOrEmpty(licenseRequest.getEmail())
                || Strings.isNullOrEmpty(licenseRequest.getUsername())
                || Strings.isNullOrEmpty(licenseRequest.getCompany())) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEMAIL_USERNAME_COMPANY_CAN_NOT_EMPTY());
        }
        if (licenseRequest.getEmail().length() > MAX_NAME_LENGTH
                || licenseRequest.getUsername().length() > MAX_NAME_LENGTH
                || licenseRequest.getCompany().length() > MAX_NAME_LENGTH) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEMAIL_USERNAME_COMPANY_IS_ILLEGAL());
        }
        if (!licenseInfoService.filterEmail(licenseRequest.getEmail())) {
            throw new KylinException(INVALID_EMAIL, MsgPicker.getMsg().getILLEGAL_EMAIL());
        }
        if (!trialPattern.matcher(licenseRequest.getCompany()).matches()
                || !trialPattern.matcher(licenseRequest.getUsername()).matches()) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getUSERNAME_COMPANY_IS_ILLEGAL());
        }

        RemoteLicenseResponse trialLicense = licenseInfoService.getTrialLicense(licenseRequest);
        if (trialLicense == null || !trialLicense.isSuccess()) {
            throw new KylinException(REMOTE_SERVER_ERROR, MsgPicker.getMsg().getLICENSE_ERROR());
        }
        licenseInfoService.updateLicense(trialLicense.getData());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }

    @ApiOperation(value = "get license info")
    @GetMapping(value = "/license/info")
    @ResponseBody
    public void requestLicense(final HttpServletResponse response) throws IOException {
        String info = licenseInfoService.requestLicenseInfo();
        File licenseInfo = File.createTempFile("license", ".info");
        FileUtils.write(licenseInfo, info, Charset.defaultCharset());
        setDownloadResponse(licenseInfo, "license.info", MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
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
                            : projectInstance.getName().toUpperCase().contains(projectName.toUpperCase())) {
                        return true;
                    }
                }
                return false;
            }).collect(Collectors.toList());
        }

        return projectInstanceList.stream().map(ProjectInstance::getName).collect(Collectors.toList());
    }

    @ApiOperation(value = "get license monitor info with detail")
    @GetMapping(value = "/capacities")
    @ResponseBody
    public EnvelopeResponse<LicenseInfoWithDetailsResponse> getLicenseMonitorInfoWithDetail(
            @RequestParam(value = "project_names", required = false, defaultValue = "") String[] projectNames,
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "capacity") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        List<String> argProjects = getValidProjects(projectNames, exactMatch);
        LicenseInfoWithDetailsResponse result;
        if (CollectionUtils.isEmpty(argProjects)) {
            result = new LicenseInfoWithDetailsResponse(0, Lists.newArrayList());
        } else {
            SourceUsageFilter sourceUsageFilter = new SourceUsageFilter(argProjects, sortBy, reverse);
            result = licenseInfoService.getLicenseMonitorInfoWithDetail(sourceUsageFilter, pageOffset, pageSize);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "get license capacity info")
    @GetMapping(value = "/license/capacity")
    @ResponseBody
    public EnvelopeResponse getLicenseCapacityInfo() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, licenseInfoService.getLicenseCapacityInfo(), "");
    }

    @ApiOperation(value = "get license node info")
    @GetMapping(value = "/license/nodes")
    @ResponseBody
    public EnvelopeResponse getLicenseNodeInfo() {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.getLicenseNodeInfo(), "");
    }

    @ApiOperation(value = "get license monitor info single project")
    @GetMapping(value = "/capacity_info")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoSingleProject(@RequestParam(value = "project") String project,
                                                               @RequestParam(value = "data_range", required = false, defaultValue = "month") String dataRange) {
        Map<Long, Long> projectCapacities = licenseInfoService.getProjectCapacities(project, dataRange);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectCapacities, "");
    }

    @ApiOperation(value = "get license monitor info in project",  notes = "Update Param: page_offset, page_size;")
    @GetMapping(value = "/capacity")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoInProject(@RequestParam(value = "project") String project,
                                                           @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
                                                           @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
                                                           @RequestParam(value = "sort_by", required = false, defaultValue = "capacity") String sortBy,
                                                           @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        aclEvaluate.checkProjectAdminPermission(project);
        SourceUsageFilter sourceUsageFilter = new SourceUsageFilter(Lists.newArrayList(), sortBy, reverse);
        ProjectCapacityResponse projectCapacityResponse = licenseInfoService.getLicenseMonitorInfoByProject(project, sourceUsageFilter);
        if (projectCapacityResponse.getSize() > 0) {
            List<CapacityDetailsResponse> tables = projectCapacityResponse.getTables();
            List<CapacityDetailsResponse> tableCapacityDetailsPaging = PagingUtil.cutPage(tables, pageOffset, pageSize);
            projectCapacityResponse.setTables(tableCapacityDetailsPaging);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectCapacityResponse, "");
    }

    @ApiOperation(value = "get last month/quarter/year license monitor info")
    @GetMapping(value = "/capacity/dashboard")
    @ResponseBody
    public EnvelopeResponse getLicenseMonitorInfoHistory(@RequestParam(value = "data_range", required = false, defaultValue = "month") String dataRange) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.getSourceUsageHistory(dataRange), "");
    }

    @ApiOperation(value = "refresh license monitor info in a project")
    @PutMapping(value = "/capacity/refresh")
    @ResponseBody
    public EnvelopeResponse refresh(@RequestParam("project") String project) {
        aclEvaluate.checkIsGlobalAdmin();
        licenseInfoService.refreshTableExtDesc(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refresh license monitor info in all projects")
    @PutMapping(value = "/capacity/refresh_all")
    @ResponseBody
    public EnvelopeResponse refreshAll() {
        aclEvaluate.checkIsGlobalAdmin();
        licenseInfoService.updateSourceUsage();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.getLicenseCapacityInfo(), "");
    }

    @PostMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody DiagPackageRequest diagPackageRequest, final HttpServletRequest request) throws Exception {
        validateDataRange(diagPackageRequest.getStart(), diagPackageRequest.getEnd());
        if (StringUtils.isEmpty(host)) {
            String uuid = systemService.dumpLocalDiagPackage(diagPackageRequest.getStart(), diagPackageRequest.getEnd(),
                    diagPackageRequest.getJobId());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, uuid, "");
        } else {
            String url = host + "/kylin/api/system/diag";
            return generateTaskForRemoteHost(request, url);
        }
    }

    @GetMapping(value = "/diag/status")
    @ResponseBody
    public EnvelopeResponse<DiagStatusResponse> getRemotePackageStatus(
            @RequestParam(value = "host", required = false) String host, @RequestParam(value = "id") String id,
            final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
            return systemService.getExtractorStatus(id);
        } else {
            String url = host + "/kylin/api/system/diag/status?id=" + id;
            return generateTaskForRemoteHost(request, url);
        }
    }

    @GetMapping(value = "/diag")
    @ResponseBody
    public void remoteDownloadPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request, final HttpServletResponse response)
            throws Exception {
        if (StringUtils.isEmpty(host)) {
            setDownloadResponse(systemService.getDiagPackagePath(id), MediaType.APPLICATION_OCTET_STREAM_VALUE,
                    response);
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            downloadFromRemoteHost(request, url, response);
        }
    }

    @DeleteMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<Boolean> remoteStopPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, systemService.stopDiagTask(id), "");
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            return generateTaskForRemoteHost(request, url);
        }
    }

    @PostMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> setReadMode(@RequestBody MaintenanceModeRequest maintenanceModeRequest) throws Exception {
        maintenanceModeService.setMaintenanceMode(maintenanceModeRequest.getReason());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unsetReadMode(@RequestParam(value = "reason") String reason) throws Exception {
        maintenanceModeService.unsetMaintenanceMode(reason);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<MaintenanceModeResponse> getMaintenanceMode() throws Exception {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, maintenanceModeService.getMaintenanceMode(), "");
    }

    @GetMapping(value = "/servers")
    @ResponseBody
    public EnvelopeResponse<ServersResponse> getServers(
            @RequestParam(value = "ext", required = false, defaultValue = "false") boolean ext) {
        val response = new ServersResponse();
        val servers = clusterManager.getServers();
        response.setStatus(maintenanceModeService.getMaintenanceMode());
        if (ext) {
            response.setServers(servers);
        } else {
            response.setServers(servers.stream().map(ServerInfoResponse::getHost).collect(Collectors.toList()));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }
}
