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

package io.kyligence.kap.metadata.epoch;

import org.apache.kylin.common.restclient.RestClient;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;

public class EpochRestClientTool {

    public static void transferUpdateEpochRequest(Epoch epoch, String project) throws IOException {
        String ownerInfo = epoch.getCurrentEpochOwner();
        transferUpdateEpochRequest(getHost(ownerInfo), getPort(ownerInfo), project);
    }

    public static void transferUpdateEpochRequest(String host, int port, String project) throws IOException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        RestClient restClient = new RestClient(host, port, authentication.getName(), authentication.getCredentials().toString());
        restClient.updatePrjEpoch(project);
    }


    public static String getHost(String ownerInfo) {
        return ownerInfo.split(":")[0];
    }

    public static int getPort(String ownerInfo) {
        return Integer.parseInt(ownerInfo.split(":")[1].split("\\|")[0]);
    }

}
