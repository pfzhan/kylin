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

package io.kyligence.kap.job.execution.stage;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.job.SegmentJob;
import io.kyligence.kap.engine.spark.job.SnapshotBuildJob;
import io.kyligence.kap.engine.spark.job.exec.BuildExec;
import io.kyligence.kap.engine.spark.job.stage.BuildParam;
import io.kyligence.kap.engine.spark.job.stage.StageExec;
import io.kyligence.kap.engine.spark.job.stage.WaiteForResource;
import io.kyligence.kap.engine.spark.job.stage.build.BuildDict;
import io.kyligence.kap.engine.spark.job.stage.build.BuildLayer;
import io.kyligence.kap.engine.spark.job.stage.build.GatherFlatTableStats;
import io.kyligence.kap.engine.spark.job.stage.build.GenerateFlatTable;
import io.kyligence.kap.engine.spark.job.stage.build.MaterializedFactTableView;
import io.kyligence.kap.engine.spark.job.stage.build.RefreshColumnBytes;
import io.kyligence.kap.engine.spark.job.stage.build.RefreshSnapshots;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionBuildDict;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionBuildLayer;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionGatherFlatTableStats;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionGenerateFlatTable;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionMaterializedFactTableView;
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionRefreshColumnBytes;
import io.kyligence.kap.engine.spark.job.stage.merge.MergeColumnBytes;
import io.kyligence.kap.engine.spark.job.stage.merge.MergeFlatTable;
import io.kyligence.kap.engine.spark.job.stage.merge.MergeIndices;
import io.kyligence.kap.engine.spark.job.stage.merge.partition.PartitionMergeColumnBytes;
import io.kyligence.kap.engine.spark.job.stage.merge.partition.PartitionMergeFlatTable;
import io.kyligence.kap.engine.spark.job.stage.merge.partition.PartitionMergeIndices;
import io.kyligence.kap.engine.spark.job.stage.snapshots.SnapshotsBuild;
import io.kyligence.kap.engine.spark.job.stage.tablesampling.AnalyzerTable;
import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public enum StageType {
    WAITE_FOR_RESOURCE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new WaiteForResource(jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForWaitingForYarnResource(ExecutableConstants.STAGE_NAME_WAITE_FOR_RESOURCE);
        }
    },
    REFRESH_SNAPSHOTS {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new RefreshSnapshots((SegmentJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_REFRESH_SNAPSHOTS);
        }
    },
    MATERIALIZED_FACT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMaterializedFactTableView((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new MaterializedFactTableView((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_MATERIALIZED_FACT_TABLE);
        }
    },
    BUILD_DICT {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionBuildDict((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new BuildDict((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_BUILD_DICT);
        }
    },
    GENERATE_FLAT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionGenerateFlatTable((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new GenerateFlatTable((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_GENERATE_FLAT_TABLE);
        }
    },
    GATHER_FLAT_TABLE_STATS {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionGatherFlatTableStats((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new GatherFlatTableStats((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_GATHER_FLAT_TABLE_STATS);
        }
    },
    BUILD_LAYER {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionBuildLayer((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new BuildLayer((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_BUILD_LAYER);
        }
    },
    REFRESH_COLUMN_BYTES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionRefreshColumnBytes((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new RefreshColumnBytes((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_REFRESH_COLUMN_BYTES);
        }
    },

    MERGE_FLAT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeFlatTable((SegmentJob) jobContext, dataSegment);
            }
            return new MergeFlatTable((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_FLAT_TABLE);
        }
    },
    MERGE_INDICES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeIndices((SegmentJob) jobContext, dataSegment);
            }
            return new MergeIndices((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_INDICES);
        }
    },
    MERGE_COLUMN_BYTES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeColumnBytes((SegmentJob) jobContext, dataSegment);
            }
            return new MergeColumnBytes((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_COLUMN_BYTES);
        }
    },
    TABLE_SAMPLING {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new AnalyzerTable((TableAnalyzerJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForTableSampling(ExecutableConstants.STAGE_NAME_TABLE_SAMPLING);
        }
    },
    SNAPSHOT_BUILD {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new SnapshotsBuild((SnapshotBuildJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForSnapshot(ExecutableConstants.STAGE_NAME_SNAPSHOT_BUILD);
        }
    };

    protected boolean isPartitioned(SparkApplication jobContext) {
        return ((SegmentJob) jobContext).isPartitioned();
    }

    public abstract StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam);

    protected abstract StageBase create(NSparkExecutable parent, KylinConfig config);

    public StageExec createStage(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam,
            BuildExec exec) {
        final StageExec step = create(jobContext, dataSegment, buildParam);
        exec.addStage(step);
        return step;
    }

    public StageBase createStage(NSparkExecutable parent, KylinConfig config) {
        final StageBase step = create(parent, config);
        parent.addStage(step);
        return step;
    }
}
