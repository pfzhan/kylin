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
package org.apache.kylin.engine.spark;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NSparkCubingJob;
import org.apache.kylin.job.execution.NSparkMergingJob;
import org.apache.kylin.job.execution.NSparkSnapshotJob;
import org.apache.kylin.job.execution.NTableSamplingJob;
import org.apache.kylin.job.execution.step.NSparkCubingStep;
import org.apache.kylin.job.util.ExecutableParaUtil;

import lombok.val;

public class ExecutableUtils {

    public static ResourceStore getRemoteStore(KylinConfig config, AbstractExecutable buildTask) {
        val buildStepUrl = ExecutableParaUtil.getOutputMetaUrl(buildTask);
        return getRemoteStore(config, buildStepUrl);
    }

    public static ResourceStore getRemoteStore(KylinConfig config, String outputMetaUrl) {
        val buildConfig = KylinConfig.createKylinConfig(config);
        buildConfig.setMetadataUrl(outputMetaUrl);
        return ResourceStore.getKylinMetaStore(buildConfig);
    }

    public static String getDataflowId(AbstractExecutable buildTask) {
        return ExecutableParaUtil.getDataflowId(buildTask);
    }

    public static Set<String> getSegmentIds(AbstractExecutable buildTask) {
        return ExecutableParaUtil.getSegmentIds(buildTask);
    }

    public static Set<Long> getLayoutIds(AbstractExecutable buildTask) {
        return ExecutableParaUtil.getLayoutIds(buildTask);

    }

    public static Set<Long> getPartitionIds(AbstractExecutable buildTask) {
        return ExecutableParaUtil.getPartitionIds(buildTask);
    }

    public static boolean needBuildSnapshots(AbstractExecutable buildTask) {
        if (buildTask instanceof NSparkCubingStep) {
            return ExecutableParaUtil.needBuildSnapshots(buildTask);
        } else {
            return false;
        }
    }

    public static void initJobFactory() {
        // register jobFactory in static function
        new NSparkCubingJob();
        new NSparkMergingJob();
        new NSparkSnapshotJob();
        new NTableSamplingJob();
    }
}
