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
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.common.persistence.transaction.AsyncAccelerateBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.persistence.transaction.RefreshVolumeBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;
import io.kyligence.kap.rest.service.AuditLogService;
import io.kyligence.kap.rest.service.KapQueryService;

@Controller
@RequestMapping(value = "/api/broadcast", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class BroadcastController extends NBasicController {

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @Autowired
    private LicenseInfoService licenseInfoService;

    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> notifyCatchUp(@RequestBody BroadcastEventReadyNotifier notifier) {
        if (notifier instanceof AuditLogBroadcastEventNotifier) {
            auditLogService.notifyCatchUp();
        } else if (notifier instanceof StopQueryBroadcastEventNotifier) {
            queryService.stopQuery(notifier.getSubject());
        } else if (notifier instanceof RefreshVolumeBroadcastEventNotifier) {
            licenseInfoService.refreshLicenseVolume();
        } else if (notifier instanceof AsyncAccelerateBroadcastEventNotifier) {
            AsyncTaskManager.cleanAccelerationTagByUser(notifier.getProject(), notifier.getSubject());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
