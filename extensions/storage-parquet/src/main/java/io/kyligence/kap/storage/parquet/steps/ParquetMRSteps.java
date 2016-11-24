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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.kv.RawTableConstants;
import io.kyligence.kap.engine.mr.steps.KapRawTableJob;
import io.kyligence.kap.engine.mr.steps.UpdateOutputDirStep;

public class ParquetMRSteps extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(ParquetMRSteps.class);

    private final String CubeIndexTmpFolderPrefix = "index";
    private final String CubeTarballTmpFolderPrefix = "tarball";
    private final String RawtableTmpFolderPrefix = "raw";

    public ParquetMRSteps(CubeSegment seg) {
        super(seg, null);
    }

    public MapReduceExecutable createMergeCuboidDataStep(CubeSegment seg, List<CubeSegment> mergingSegments, String jobID, Class<? extends AbstractHadoopJob> clazz) {

        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getParquetFolderPath(merging) + "*");
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = getParquetFolderPath(seg);

        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(clazz);
        return mergeCuboidDataStep;
    }

    public MapReduceExecutable createRawTableStep() {
        MapReduceExecutable rawTableStep = new MapReduceExecutable();

        RawTableInstance instance = detectRawTable();
        RawTableSegment rawSeg = null;
        try {
            rawSeg = RawTableManager.getInstance(seg.getConfig()).appendSegment(instance, seg);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        String parquentStoragePath = KapConfig.wrap(seg.getConfig()).getParquentStoragePath();
        String output = parquentStoragePath + rawSeg.getRawTableInstance().getUuid() + "/" + rawSeg.getUuid() + "/";
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        rawTableStep.setName(RawTableConstants.BUILD_RAWTABLE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, instance.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, rawSeg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, output + RawtableTmpFolderPrefix);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Raw_Table_Builder_" + instance.getName());

        rawTableStep.setMapReduceParams(cmd.toString());
        rawTableStep.setMapReduceJobClass(KapRawTableJob.class);
        rawTableStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return rawTableStep;
    }

    public MapReduceExecutable createMergeRawDataStep(CubeSegment seg, String jobID, Class<? extends AbstractHadoopJob> clazz) {
        final List<String> mergingRawTableSegmetPaths = Lists.newArrayList();
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig()).getRawTableInstance(seg.getRealization().getName());
        RawTableSegment rawSegment = null;
        try {
            rawSegment = RawTableManager.getInstance(rawInstance.getConfig()).appendSegment(rawInstance, seg);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        List<RawTableSegment> mergingRawSegments = rawInstance.getMergingSegments(rawSegment);
        Preconditions.checkState(mergingRawSegments.size() > 1, "there should be more than 2 segments to merge, target segment " + rawSegment);

        for (RawTableSegment merging : mergingRawSegments) {
            mergingRawTableSegmetPaths.add(getRawParquetFolderPath(merging) + "RawTable/*");
        }
        String formattedPath = StringUtil.join(mergingRawTableSegmetPaths, ",");
        String outputPath = getRawParquetFolderPath(rawSegment) + "RawTable/";

        MapReduceExecutable mergeRawDataStep = new MapReduceExecutable();
        mergeRawDataStep.setName("Merge Raw Table Data");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_RawTable_" + seg.getCubeInstance().getName() + "_Step");

        mergeRawDataStep.setMapReduceParams(cmd.toString());
        mergeRawDataStep.setMapReduceJobClass(clazz);
        return mergeRawDataStep;
    }

    public MapReduceExecutable createParquetPageIndex(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Columnar Page Index");
        result.setMapReduceJobClass(ParquetPageIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Columnar_Page_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getParquetIndexTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public UpdateOutputDirStep createParquetPageIndexClenup(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getParquetFolderPath(seg));
        result.setSubdirFilter(CubeIndexTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Cube Index Output");
        return result;
    }

    public MapReduceExecutable createRawTableParquetPageIndex(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig()).getRawTableInstance(seg.getRealization().getName());
        RawTableSegment rawSeg = rawInstance.getSegmentById(seg.getUuid());
        result.setName("Build Raw Table Columnar Page Index");
        result.setMapReduceJobClass(RawTablePageIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Raw_Table_Parquet_Page_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, rawInstance.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, rawSeg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getRawParquetFolderPath(rawSeg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawParquetFolderPath(rawSeg)); // just tmp files

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public UpdateOutputDirStep createRawTableCleanupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getParquetRawFolderPath(seg));
        result.setSubdirFilter(RawtableTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Rawtable Output");
        return result;
    }

    public UpdateOutputDirStep createParquetTarballCleaupJob(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getParquetFolderPath(seg));
        result.setSubdirFilter(CubeTarballTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Cube Tarball Output");
        return result;
    }

    public MapReduceExecutable createRawTableParquetPageFuzzyIndex(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig()).getRawTableInstance(seg.getRealization().getName());
        RawTableSegment rawSeg = rawInstance.getSegmentById(seg.getUuid());
        result.setName("Build Raw Table Columnar Fuzzy Index");
        result.setMapReduceJobClass(RawTableFuzzyIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Raw_Table_Parquet_Fuzzy_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, rawInstance.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, rawSeg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getRawParquetFolderPath(rawSeg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawParquetFolderPath(rawSeg)); // just tmp files

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public MapReduceExecutable createParquetTarballJob(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Tarball Columnar Files");
        result.setMapReduceJobClass(ParquetTarballJob.class);
        result.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Tarball_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getParquetCubeTarballFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public List<String> getMergingSegmentsParquetFolders() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> ret = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            ret.add(getParquetFolderPath(merging));
        }
        return ret;
    }

    public List<String> getMergingSegmentJobWorkingDirs() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHDFSPaths.add(getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }

    public void addMergingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {

        //clean two parts: 1.parquet storage folders 2. working dirs
        List<String> toCleanFolders = getMergingSegmentsParquetFolders();
        toCleanFolders.addAll(getMergingSegmentJobWorkingDirs());

        logger.info("toCleanFolders are :" + toCleanFolders);

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(null);//delete all source folder for merge

        jobFlow.addTask(step);
    }

    public void addCubingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        List<String> toCleanFolders = Lists.newArrayList(getParquetFolderPath(seg));
        List<String> toCleanFileSuffixs = Lists.newArrayList(".parquet", ".parquet.inv", ".tmp");

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(toCleanFileSuffixs);

        jobFlow.addTask(step);
    }

    public ParquetShardSizingStep createParquetShardSizingStep(String jobId) {
        ParquetShardSizingStep result = new ParquetShardSizingStep();
        result.setName("Sizing Columnar Shards");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    public RawShardSizingStep createRawShardSizingStep(String jobId) {
        RawShardSizingStep result = new RawShardSizingStep();
        result.setName("Sizing Raw Shards");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    private String getParquetFolderPath(CubeSegment cubeSegment) {
        return new StringBuffer(KapConfig.wrap(config.getConfig()).getParquentStoragePath()).append(cubeSegment.getCubeInstance().getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }

    private String getParquetRawFolderPath(CubeSegment cubeSegment) {
        RawTableInstance instance = detectRawTable();
        RawTableSegment rawSeg;
        rawSeg = RawTableManager.getInstance(seg.getConfig()).getRawtableSegmentByDataRange(instance, seg.getDateRangeStart(), seg.getDateRangeEnd()).get(0);
        String parquentStoragePath = KapConfig.wrap(seg.getConfig()).getParquentStoragePath();
        String output = parquentStoragePath + rawSeg.getRawTableInstance().getUuid() + "/" + rawSeg.getUuid() + "/";
        return output;
    }

    private String getParquetIndexTmpFolderPath(CubeSegment cubeSegment) {
        return getParquetFolderPath(cubeSegment) + "/" + CubeIndexTmpFolderPrefix;
    }

    private String getParquetCubeTarballFolderPath(CubeSegment cubeSegment) {
        return getParquetFolderPath(cubeSegment) + "/" + CubeTarballTmpFolderPrefix;
    }

    private String getRawParquetFolderPath(RawTableSegment rawSegment) {
        return new StringBuffer(KapConfig.wrap(config.getConfig()).getParquentStoragePath()).append(rawSegment.getRawTableInstance().getUuid()).append("/").append(rawSegment.getUuid()).append("/").toString();
    }

    private RawTableInstance detectRawTable() {
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig()).getRawTableInstance(seg.getRealization().getName());
        logger.info("Raw table is " + (rawInstance == null ? "not " : "") + "specified in this cubing job " + seg);
        return rawInstance;
    }
}
