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

package io.kyligence.kap.storage.parquet.steps;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.engine.mr.steps.ByteArrayShardCuboidPartitioner;
import io.kyligence.kap.engine.mr.steps.KapMergeCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapMergeRawTableJob;
import io.kyligence.kap.engine.mr.steps.ShardCuboidPartitioner;
import io.kyligence.kap.storage.parquet.format.ParquetCubeOutputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.HiveToBaseCuboidMapper;
import org.apache.kylin.engine.mr.steps.InMemCuboidMapper;
import org.apache.kylin.engine.mr.steps.NDCuboidMapper;
import org.apache.kylin.engine.mr.steps.ReducerNumSizing;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ParquetMROutput2 implements IMROutput2 {

    private static final Logger logger = LoggerFactory.getLogger(ParquetMROutput2.class);

    @Override
    public IMROutput2.IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchCubingOutputSide2() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig())
                    .getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubeShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                //                jobFlow.addTask(steps.createCubePageIndexStep(jobFlow.getId()));
                //                jobFlow.addTask(steps.createCubePageIndexCleanupStep(jobFlow.getId()));
                //                jobFlow.addTask(steps.createCubeTarballStep(jobFlow.getId()));
                //                jobFlow.addTask(steps.createCubeTarballCleaupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeInfoCollectionStep(jobFlow.getId(), seg));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createRawtableShardSizingStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexCleanupStep(jobFlow.getId()));
                }
                jobFlow.addTask(steps.createStorageDuplicateStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubeGarbageCollectionSteps(jobFlow);
            }

            @Override
            public IMROutputFormat getOuputFormat() {
                return new ParquetMROutputFormat();
            }
        };
    }

    public static class ParquetMROutputFormat implements IMROutputFormat {

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(ParquetTarballFileInputFormat.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment, int level) throws Exception {
            int reducerNum = 1;
            Class mapperClass = job.getMapperClass();
            // set partitioner
            if (mapperClass == InMemCuboidMapper.class) {
                // inmem
                job.setPartitionerClass(ByteArrayShardCuboidPartitioner.class);
                reducerNum = ReducerNumSizing.getInmemCubingReduceTaskNum(segment);
            } else if (mapperClass == NDCuboidMapper.class || mapperClass == HiveToBaseCuboidMapper.class) {
                // layer
                job.setPartitionerClass(ShardCuboidPartitioner.class);
                reducerNum = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment,
                        AbstractHadoopJob.getTotalMapInputMB(job), level);
                List<List<Long>> layeredCuboids = segment.getCuboidScheduler().getCuboidsByLayer();
                for (Long cuboidId : layeredCuboids.get(level)) {
                    reducerNum = Math.max(reducerNum, segment.getCuboidShardNum(cuboidId));
                }
            }
            job.setNumReduceTasks(reducerNum);

            Path outputPath = new Path(output);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(ParquetCubeOutputFormat.class);
        }
    }

    @Override
    public IMROutput2.IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchMergeOutputSide2() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig())
                    .getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubeShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments,
                                                DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(
                        steps.createCubeMergeStep(seg, mergingSegments, jobFlow.getId(), KapMergeCuboidJob.class));
                jobFlow.addTask(steps.createCubeMergeCleanupStep(jobFlow.getId(), seg));
                jobFlow.addTask(steps.createCubePageIndexStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubePageIndexCleanupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballCleaupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeInfoCollectionStep(jobFlow.getId(), seg));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createRawtableMergeStep(seg, jobFlow.getId(), KapMergeRawTableJob.class));
                    jobFlow.addTask(steps.createRawtableMergeCleanupStep(jobFlow.getId(), seg));
                    jobFlow.addTask(steps.createRawtablePageIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexCleanupStep(jobFlow.getId()));
                }
                jobFlow.addTask(steps.createStorageDuplicateStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubeGarbageCollectionSteps(jobFlow);
                steps.addMergeGarbageCollectionSteps(jobFlow);
            }

            @Override
            public IMRMergeOutputFormat getOuputFormat() {
                return new ParquetMRMergeOutputFormat();
            }
        };
    }

    public static class ParquetMRMergeOutputFormat implements IMRMergeOutputFormat {

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(ParquetTarballFileInputFormat.class);
            FileInputFormat.setInputPathFilter(job, CuboidPathFilter.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment) throws Exception {
            int reducerNum = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment,
                    AbstractHadoopJob.getTotalMapInputMB(job), -1);
            job.setPartitionerClass(ShardCuboidPartitioner.class);
            Set<Long> allCuboids = segment.getCuboidScheduler().getAllCuboidIds();
            for (Long cuboidId : allCuboids) {
                reducerNum = Math.max(reducerNum, segment.getCuboidShardNum(cuboidId));
            }
            job.setNumReduceTasks(reducerNum);

            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(ParquetCubeOutputFormat.class);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
        }

        @Override
        public CubeSegment findSourceSegment(FileSplit fileSplit, CubeInstance cube) {
            Path path = fileSplit.getPath();
            String segmentID = path.getParent().getParent().getName();
            logger.info("Identified segment id for current input split is " + segmentID);
            return cube.getSegmentById(segmentID);
        }

        public static class CuboidPathFilter implements PathFilter {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                boolean ret = !(name.endsWith(".parquet") || name.endsWith(".inv"));
                return ret;
            }

        }
    }
}
