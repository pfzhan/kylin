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

package io.kyligence.kap.engine.spark.job;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageStepFactory;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;

public enum JobStepType {

    RESOURCE_DETECT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NResourceDetectStep(parent);
        }
    },

    CLEAN_UP_AFTER_MERGE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            AbstractExecutable step = new NSparkCleanupAfterMergeStep();
            return step;

        }
    },
    CUBING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }
    },
    MERGING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }
    },

    BUILD_SNAPSHOT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkSnapshotBuildingStep(config.getSnapshotBuildClassName());
        }
    },

    SAMPLING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }
    },

    UPDATE_METADATA {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            if (!(parent instanceof DefaultChainedExecutableOnModel)) {
                throw new IllegalArgumentException();
            }
            ((DefaultChainedExecutableOnModel) parent).setHandler(
                    ExecutableHandlerFactory.createExecutableHandler((DefaultChainedExecutableOnModel) parent));
            return new NSparkUpdateMetadataStep();
        }
    },

    SECOND_STORAGE_EXPORT {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageLoadStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }
    },

    SECOND_STORAGE_REFRESH {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageRefreshStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }
    },

    SECOND_STORAGE_MERGE {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageMergeStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }
    };

    protected abstract AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config);

    public AbstractExecutable createStep(DefaultChainedExecutable parent, KylinConfig config) {
        AbstractExecutable step = create(parent, config);
        addParam(parent, step, config);
        return step;
    }

    protected void addParam(DefaultChainedExecutable parent, AbstractExecutable step, KylinConfig config) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            ((NSparkExecutable) step).setDistMetaUrl(config.getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
        if (CollectionUtils.isNotEmpty(parent.getTargetPartitions())) {
            step.setTargetPartitions(parent.getTargetPartitions());
        }
    }

}
