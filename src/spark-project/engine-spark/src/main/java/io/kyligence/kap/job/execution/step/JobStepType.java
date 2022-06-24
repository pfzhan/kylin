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

package io.kyligence.kap.job.execution.step;

import static io.kyligence.kap.job.execution.stage.StageType.BUILD_DICT;
import static io.kyligence.kap.job.execution.stage.StageType.BUILD_LAYER;
import static io.kyligence.kap.job.execution.stage.StageType.GATHER_FLAT_TABLE_STATS;
import static io.kyligence.kap.job.execution.stage.StageType.GENERATE_FLAT_TABLE;
import static io.kyligence.kap.job.execution.stage.StageType.MATERIALIZED_FACT_TABLE;
import static io.kyligence.kap.job.execution.stage.StageType.MERGE_COLUMN_BYTES;
import static io.kyligence.kap.job.execution.stage.StageType.MERGE_FLAT_TABLE;
import static io.kyligence.kap.job.execution.stage.StageType.MERGE_INDICES;
import static io.kyligence.kap.job.execution.stage.StageType.REFRESH_COLUMN_BYTES;
import static io.kyligence.kap.job.execution.stage.StageType.REFRESH_SNAPSHOTS;
import static io.kyligence.kap.job.execution.stage.StageType.SNAPSHOT_BUILD;
import static io.kyligence.kap.job.execution.stage.StageType.TABLE_SAMPLING;
import static io.kyligence.kap.job.execution.stage.StageType.WAITE_FOR_RESOURCE;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.NTableSamplingJob;
import io.kyligence.kap.job.execution.handler.ExecutableHandlerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.job.SecondStorageStepFactory;

@Slf4j
public enum JobStepType {

    RESOURCE_DETECT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            if (config.getSparkEngineBuildStepsToSkip().contains(NResourceDetectStep.class.getName())) {
                return null;
            }
            return new NResourceDetectStep(parent);
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_AFTER_MERGE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            AbstractExecutable step = new NSparkCleanupAfterMergeStep();
            return step;

        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },
    CUBING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            REFRESH_SNAPSHOTS.createStage(parent, config);

            MATERIALIZED_FACT_TABLE.createStage(parent, config);
            BUILD_DICT.createStage(parent, config);
            GENERATE_FLAT_TABLE.createStage(parent, config);
            GATHER_FLAT_TABLE_STATS.createStage(parent, config);
            BUILD_LAYER.createStage(parent, config);
            REFRESH_COLUMN_BYTES.createStage(parent, config);
        }
    },
    MERGING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);

            MERGE_FLAT_TABLE.createStage(parent, config);
            MERGE_INDICES.createStage(parent, config);
            MERGE_COLUMN_BYTES.createStage(parent, config);
        }
    },

    BUILD_SNAPSHOT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkSnapshotBuildingStep(config.getSnapshotBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            SNAPSHOT_BUILD.createStage(parent, config);
        }
    },

    SAMPLING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            TABLE_SAMPLING.createStage(parent, config);
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

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
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

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
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

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
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

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_TRANSACTIONAL_TABLE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new SparkCleanupTransactionalTableStep();
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    };

    protected abstract AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config);

    /** add stage in spark executable */
    protected abstract void addSubStage(NSparkExecutable parent, KylinConfig config);

    public AbstractExecutable createStep(DefaultChainedExecutable parent, KylinConfig config) {
        AbstractExecutable step = create(parent, config);
        if (step == null) {
            log.info("{} skipped", this);
        } else {
            addParam(parent, step);
        }
        return step;
    }

    protected void addParam(DefaultChainedExecutable parent, AbstractExecutable step) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            addSubStage((NSparkExecutable) step, KylinConfig.readSystemKylinConfig());
            ((NSparkExecutable) step).setStageMap();

            ((NSparkExecutable) step).setDistMetaUrl(
                    KylinConfig.readSystemKylinConfig().getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
        if (CollectionUtils.isNotEmpty(parent.getTargetPartitions())) {
            step.setTargetPartitions(parent.getTargetPartitions());
        }
    }

}
