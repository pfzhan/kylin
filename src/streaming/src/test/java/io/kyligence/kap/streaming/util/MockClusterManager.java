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
package io.kyligence.kap.streaming.util;

import io.kyligence.kap.cluster.AvailableResource;
import io.kyligence.kap.cluster.ContainerLimit;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.cluster.ResourceInfo;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Set;

public class MockClusterManager implements IClusterManager {

    @Override
    public ResourceInfo fetchMaximumResourceAllocation() {
        return null;
    }

    @Override
    public AvailableResource fetchQueueAvailableResource(String queueName) {
        return null;
    }

    @Override
    public String getBuildTrackingUrl(SparkSession sparkSession) {
        return null;
    }

    @Override
    public void killApplication(String jobStepId) {

    }

    @Override
    public void killApplication(String jobStepPrefix, String jobStepId) {

    }

    @Override
    public boolean isApplicationBeenKilled(String applicationId) {
        return false;
    }

    @Override
    public List<String> getRunningJobs(Set<String> queues) {
        return null;
    }

    @Override
    public ResourceInfo fetchQueueStatistics(String queueName) {
        return null;
    }

    @Override
    public boolean applicationExisted(String jobId) {
        return false;
    }

    @Override
    public ContainerLimit fetchContainerLimit() {
        return null;
    }
}