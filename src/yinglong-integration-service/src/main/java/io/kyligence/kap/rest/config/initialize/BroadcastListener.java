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
package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.persistence.transaction.AccessBatchGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AccessGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AccessRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclGrantEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AclTCRRevokeEventNotifier;
import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.persistence.transaction.EpochCheckBroadcastNotifier;
import io.kyligence.kap.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.broadcaster.Broadcaster;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.AuditLogService;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BroadcastListener {

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    private AclTCRService aclTCRService;

    @Autowired
    private AccessService accessService;

    private Broadcaster broadcaster = Broadcaster.getInstance(KylinConfig.getInstanceFromEnv(), this);

    @Subscribe
    public void onEventReady(BroadcastEventReadyNotifier notifier) {
        broadcaster.announce(notifier);
    }

    public void handle(BroadcastEventReadyNotifier notifier) throws IOException {
        log.info("accept broadcast Event {}", notifier);
        if (notifier instanceof AuditLogBroadcastEventNotifier) {
            auditLogService.notifyCatchUp();
        } else if (notifier instanceof StopQueryBroadcastEventNotifier) {
            queryService.stopQuery(notifier.getSubject());
        } else if (notifier instanceof EpochCheckBroadcastNotifier) {
            EpochManager.getInstance().updateAllEpochs();
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
        } else if (notifier instanceof AclTCRRevokeEventNotifier) {
            AclTCRRevokeEventNotifier aclTCRRevokeEventNotifier = (AclTCRRevokeEventNotifier) notifier;
            aclTCRService.revokeAclTCR(aclTCRRevokeEventNotifier.getSid(), aclTCRRevokeEventNotifier.isPrinciple());
        }
    }
}
