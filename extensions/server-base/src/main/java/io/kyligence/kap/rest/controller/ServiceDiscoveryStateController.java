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

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.rest.service.ServiceDiscoveryStateService;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.Participant;

@Controller
@RequestMapping(value = "/service_discovery/state")
public class ServiceDiscoveryStateController extends BasicController {

    @Autowired
    @Qualifier("serviceDiscoveryStateService")
    ServiceDiscoveryStateService serviceDiscoveryStateService;

    @RequestMapping(value = "/is_job_server", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public boolean hasDefaultSchedulerStarted() throws UnknownHostException {
        return serviceDiscoveryStateService.hasDefaultSchedulerStarted();
    }

    @RequestMapping(value = "/all", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<ServiceDiscoveryState> getAllState(@RequestHeader("Authorization") String basicAuthen) throws UnknownHostException {
        String[] kapServers = KylinConfig.getInstanceFromEnv().getRestServers();
        List<Participant> allParticipants = serviceDiscoveryStateService.getAllParticipants();
        Map<String, String> jobServerState = serviceDiscoveryStateService.getJobServerState(basicAuthen, kapServers);

        List<String> participants = new ArrayList<>();
        List<String> leaders = new ArrayList<>();
        List<String> jobServers = new ArrayList<>();

        for (Participant p : allParticipants) {
            participants.add(p.getId());
            if (p.isLeader()) {
                leaders.add(p.getId());
            }
        }

        for (String server : jobServerState.keySet()) {
            if (jobServerState.get(server).equals("true")) {
                jobServers.add(server);
            }
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, new ServiceDiscoveryState(Arrays.asList(kapServers), participants, leaders, jobServers), "get service discovery's all state");
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class ServiceDiscoveryState implements Serializable {
        @JsonProperty()
        List<String> allNodes;
        @JsonProperty()
        List<String> leaderSelectionParticipants;
        @JsonProperty()
        List<String> selectedLeaders;
        @JsonProperty()
        List<String> activeJobServers;

        ServiceDiscoveryState(List<String> allNodes, List<String> participants, List<String> leaders, List<String> jobServers) {
            this.allNodes = allNodes;
            this.leaderSelectionParticipants = participants;
            this.selectedLeaders = leaders;
            this.activeJobServers = jobServers;
        }
    }
}
