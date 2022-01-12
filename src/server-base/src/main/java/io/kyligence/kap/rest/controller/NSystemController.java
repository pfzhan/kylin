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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
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

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.common.persistence.transaction.EpochCheckBroadcastNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.request.DiagPackageRequest;
import io.kyligence.kap.rest.request.DiagProgressRequest;
import io.kyligence.kap.rest.request.MaintenanceModeRequest;
import io.kyligence.kap.rest.request.QueryDiagPackageRequest;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.rest.response.MaintenanceModeResponse;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import io.kyligence.kap.rest.response.ServersResponse;
import io.kyligence.kap.rest.service.MaintenanceModeService;
import io.kyligence.kap.rest.service.SystemService;
import io.kyligence.kap.tool.util.ToolUtil;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NSystemController extends NBasicController {

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

    @VisibleForTesting
    public void setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
    }

    @VisibleForTesting
    public AclEvaluate getAclEvaluate() {
        return this.aclEvaluate;
    }


    @ApiOperation(value = "diag", tags = { "SM" })
    @PostMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody DiagPackageRequest diagPackageRequest, final HttpServletRequest request) throws Exception {
        validateDataRange(diagPackageRequest.getStart(), diagPackageRequest.getEnd());
        if (StringUtils.isEmpty(host)) {
            String uuid = systemService.dumpLocalDiagPackage(diagPackageRequest.getStart(), diagPackageRequest.getEnd(),
                    diagPackageRequest.getJobId());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, uuid, "");
        } else {
            String url = host + "/kylin/api/system/diag";
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "queryDiag", tags = { "QE" })
    @PostMapping(value = "/diag/query")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpQueryDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody QueryDiagPackageRequest queryDiagPackageRequest, final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
            String uuid = systemService.dumpLocalQueryDiagPackage(queryDiagPackageRequest.getQueryId(), queryDiagPackageRequest.getProject());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, uuid, "");
        } else {
            String url = host + "/kylin/api/system/diag/query";
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "diagProgress", tags = { "SM" })
    @PutMapping(value = "/diag/progress")
    @ResponseBody
    public EnvelopeResponse<String> updateDiagProgress(@RequestBody DiagProgressRequest diagProgressRequest) {
        systemService.updateDiagProgress(diagProgressRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/roll_event_log")
    @ResponseBody
    public EnvelopeResponse<String> rollEventLog() {
        if (ToolUtil.waitForSparderRollUp()) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        }
        return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "Rollup sparder eventLog failed.");
    }

    @ApiOperation(value = "diagStatus", tags = { "SM" })
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

    @ApiOperation(value = "diagDownload", tags = { "SM" })
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

    @ApiOperation(value = "cancelDiag", tags = { "SM" })
    @DeleteMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> remoteStopPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
            systemService.stopDiagTask(id);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "enterMaintenance", tags = { "DW" })
    @PostMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> setMaintenanceMode(@RequestBody MaintenanceModeRequest maintenanceModeRequest) {
        maintenanceModeService.setMaintenanceMode(maintenanceModeRequest.getReason());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "exitMaintenance", tags = { "DW" })
    @DeleteMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unsetReadMode(@RequestParam(value = "reason") String reason) {
        maintenanceModeService.unsetMaintenanceMode(reason);
        EventBusFactory.getInstance().postAsync(new EpochCheckBroadcastNotifier());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMaintenance", tags = { "DW" })
    @GetMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<MaintenanceModeResponse> getMaintenanceMode() throws Exception {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, maintenanceModeService.getMaintenanceMode(), "");
    }

    @ApiOperation(value = "servers", tags = { "DW" })
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "host", tags = { "DW" })
    @GetMapping(value = "/host")
    @ResponseBody
    public EnvelopeResponse<String> getHostname() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, AddressUtil.getLocalInstance(), "");
    }

    @ApiOperation(value = "reload metadata", tags = { "MID" })
    @PostMapping(value = "/metadata/reload")
    @ResponseBody
    public EnvelopeResponse<String> reloadMetadata() {
        systemService.reloadMetadata();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}
