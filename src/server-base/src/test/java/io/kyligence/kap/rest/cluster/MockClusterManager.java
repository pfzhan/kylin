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
package io.kyligence.kap.rest.cluster;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.ClusterConstant;
import io.kyligence.kap.rest.response.ServerInfoResponse;

import java.util.List;

public class MockClusterManager implements ClusterManager {
    @Override
    public String getLocalServer() {
        return "127.0.0.1:7070";
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return Lists.newArrayList(new ServerInfoResponse("127.0.0.1:7070", ClusterConstant.QUERY),
                new ServerInfoResponse("127.0.0.1:7071", ClusterConstant.QUERY));
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return Lists.newArrayList(new ServerInfoResponse("127.0.0.1:7070", ClusterConstant.ALL));
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return Lists.newArrayList(new ServerInfoResponse("127.0.0.1:7070", ClusterConstant.ALL));
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return Lists.newArrayList(new ServerInfoResponse("127.0.0.1:7070", ClusterConstant.ALL));
    }
}
