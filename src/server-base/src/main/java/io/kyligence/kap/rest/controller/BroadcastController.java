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

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.common.persistence.transaction.AccessBatchGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AccessGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AccessRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclTCRRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.persistence.transaction.EpochCheckBroadcastNotifier;
import io.kyligence.kap.common.persistence.transaction.RefreshVolumeBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.UpdateJobStatusEventNotifier;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.AuditLogService;
import io.kyligence.kap.rest.service.JobService;

@Controller
@RequestMapping(value = "/api/broadcast", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class BroadcastController extends NBasicController {

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Autowired
    private AclTCRService aclTCRService;

    @Autowired
    private AccessService accessService;

    @Autowired
    private JobService jobService;

    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> broadcastReceive(@RequestBody BroadcastEventReadyNotifier notifier)
            throws IOException {
        if (notifier instanceof AuditLogBroadcastEventNotifier) {
            auditLogService.notifyCatchUp();
        } else if (notifier instanceof StopQueryBroadcastEventNotifier) {
            queryService.stopQuery(notifier.getSubject());
        } else if (notifier instanceof RefreshVolumeBroadcastEventNotifier) {
            licenseInfoService.refreshLicenseVolume();
        } else if (notifier instanceof EpochCheckBroadcastNotifier) {
            EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).updateAllEpochs();
        } else if (notifier instanceof SourceUsageUpdateNotifier) {
            licenseInfoService.updateSourceUsage();
        } else if (notifier instanceof AclGrantEventNotifier) {
            aclTCRService.updateAclFromRemote((AclGrantEventNotifier) notifier, null);
        } else if (notifier instanceof AclRevokeEventNotifier) {
            aclTCRService.updateAclFromRemote(null, (AclRevokeEventNotifier) notifier);
        } else if (notifier instanceof AccessGrantEventNotifier) {
            accessService.updateAccessFromRemote((AccessGrantEventNotifier) notifier, null, null);
        } else if (notifier instanceof AccessBatchGrantEventNotifier) {
            accessService.updateAccessFromRemote(null, (AccessBatchGrantEventNotifier) notifier, null);
        } else if (notifier instanceof AccessRevokeEventNotifier) {
            accessService.updateAccessFromRemote(null, null, (AccessRevokeEventNotifier) notifier);
        } else if (notifier instanceof UpdateJobStatusEventNotifier) {
            UpdateJobStatusEventNotifier updateJobStatusEventNotifier = (UpdateJobStatusEventNotifier) notifier;
            jobService.batchUpdateGlobalJobStatus(updateJobStatusEventNotifier.getJobIds(),
                    updateJobStatusEventNotifier.getAction(), updateJobStatusEventNotifier.getStatuses());
        } else if (notifier instanceof AclTCRRevokeEventNotifier) {
            AclTCRRevokeEventNotifier aclTCRRevokeEventNotifier = (AclTCRRevokeEventNotifier) notifier;
            aclTCRService.revokeAclTCR(aclTCRRevokeEventNotifier.getSid(), aclTCRRevokeEventNotifier.isPrinciple());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
