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

package io.kyligence.kap.rest.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.kyligence.kap.job.impl.curator.CuratorLeaderSelector;
import io.kyligence.kap.job.impl.curator.CuratorScheduler;
import io.kyligence.kap.rest.client.KAPRESTClient;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.Participant;

@Component("serviceDiscoveryStateService")
public class ServiceDiscoveryStateService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscoveryStateService.class);

    public List<Participant> getAllParticipants() {
        CuratorLeaderSelector leaderSelector = CuratorScheduler.getLeaderSelector();
        if (leaderSelector != null) {
            return leaderSelector.getParticipants();
        } else {
            return new ArrayList<>();
        }
    }

    public boolean hasDefaultSchedulerStarted() {
        CuratorLeaderSelector leaderSelector = CuratorScheduler.getLeaderSelector();
        return leaderSelector != null && leaderSelector.hasDefaultSchedulerStarted();
    }

    public Map<String, String> getJobServerState(String basicAuthen, String[] servers) {
        Map<String, String> r = new HashMap<>();
        for (String server : servers) {
            logger.debug("Ask server: " + server + " for its job server state");
            KAPRESTClient client = new KAPRESTClient(server, basicAuthen);
            Pair<String, String> jobServerState = client.getJobServerState();
            r.put(jobServerState.getFirst(), jobServerState.getSecond());
        }
        return r;
    }
}
