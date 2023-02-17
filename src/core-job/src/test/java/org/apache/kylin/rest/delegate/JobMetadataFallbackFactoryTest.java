/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.delegate;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.concurrent.TimeoutException;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class JobMetadataFallbackFactoryTest {

    @Test
    void disableFallbackFactoryTest() {
        getTestConfig().setProperty("kylin.server.common-only", "false");
        Exception exception = new TimeoutException();
        JobMetadataFallbackFactory fallbackFactory = new JobMetadataFallbackFactory();
        Assertions.assertThrows(TimeoutException.class, () -> fallbackFactory.create(exception));
    }

    @Test
    void fallbackFactoryTest() {
        getTestConfig().setProperty("kylin.server.common-only", "true");
        Exception exception = new TimeoutException();
        JobMetadataFallbackFactory fallbackFactory = new JobMetadataFallbackFactory();

        JobMetadataRpc rpc = fallbackFactory.create(exception);
        Assertions.assertThrows(TimeoutException.class, () -> rpc.addIndexJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.addJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.addRelatedIndexJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.addSecondStorageJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.addSegmentJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.buildPartitionJob(new JobMetadataRequest()));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.refreshSegmentJob(new JobMetadataRequest(), false));
        Assertions.assertThrows(TimeoutException.class, () -> rpc.mergeSegmentJob(new JobMetadataRequest()));

        try {
            rpc.checkSuicideJobOfModel(null, null);
            rpc.clearJobsByProject(null);
            rpc.countByModelAndStatus(null, null, null, null, null);
            rpc.deleteJobByIdList(null, null);
            rpc.discardJob(null, null);
            rpc.restoreJobInfo(null, null, false);
            rpc.stopBatchJob(null, null);
            rpc.fetchAllJobLock();
            rpc.fetchJobList(null);
            rpc.fetchNotFinalJobsByTypes(null, null, null);
            rpc.getExecutablePOsByFilter(null);
            rpc.getExecutablePOsByStatus(null, null);
            rpc.getJobExecutablesPO(null);
            rpc.getLayoutsByRunningJobs(null, null);
            rpc.listExecPOByJobTypeAndStatus(null, null, null);
            rpc.listPartialExec(null, null, null);
        } catch (Throwable ignore) {
            Assertions.fail();
        }
    }
}
