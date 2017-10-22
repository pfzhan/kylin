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

    private final String CubePageIndexTmpFolderPrefix = "cube_page_index";
    private final String CubeTarballTmpFolderPrefix = "cube_tarball";
    private final String RawtableTmpFolderPrefix = "rawtable";
    private final String RawtablePageIndexTmpFolderPrefix = "raw_page_index";
    private final String RawtableFuzzyIndexTmpFolderPrefix = "raw_fuzzy_index";
    private final String CubeMergeTmpFolderPrefix = "cube_merge";
    private final String RawtableMergeTmpFolderPrefix = "raw_merge";

    public ParquetMRSteps(CubeSegment seg) {
        super(seg, null);
    }

    public MapReduceExecutable createCubeMergeStep(CubeSegment seg, List<CubeSegment> mergingSegments, String jobID, Class<? extends AbstractHadoopJob> clazz) {

        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getCubeFolderPath(merging) + "*");
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");

        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getCubeMergeTmpFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(clazz);
        return mergeCuboidDataStep;
    }

    public MapReduceExecutable createRawtableStep(String jobId) {
        MapReduceExecutable rawTableStep = new MapReduceExecutable();

        RawTableInstance instance = detectRawTable(seg);
        RawTableSegment rawSeg;
        try {
            rawSeg = RawTableManager.getInstance(seg.getConfig()).appendSegment(instance, seg);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);
        rawTableStep.setName(RawTableConstants.BUILD_RAWTABLE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, instance.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, rawSeg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawtableTmpFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Raw_Table_Builder_" + instance.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        rawTableStep.setMapReduceParams(cmd.toString());
        rawTableStep.setMapReduceJobClass(KapRawTableJob.class);
        return rawTableStep;
    }

    public UpdateOutputDirStep createCubeMergeCleanupStep(String jobId, CubeSegment cubeSegment) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getCubeFolderPath(cubeSegment));
        result.setSubdirFilter(CubeMergeTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Cube Merge Output");
        return result;
    }

    public MapReduceExecutable createRawtableMergeStep(CubeSegment seg, String jobID, Class<? extends AbstractHadoopJob> clazz) {
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
            mergingRawTableSegmetPaths.add(getRawTableFolderPath(merging) + "RawTable/*");
        }
        String formattedPath = StringUtil.join(mergingRawTableSegmetPaths, ",");

        MapReduceExecutable mergeRawDataStep = new MapReduceExecutable();
        mergeRawDataStep.setName("Merge Raw Table Data");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawtableMergeTmpFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_RawTable_" + seg.getCubeInstance().getName() + "_Step");

        mergeRawDataStep.setMapReduceParams(cmd.toString());
        mergeRawDataStep.setMapReduceJobClass(clazz);
        return mergeRawDataStep;
    }

    public MapReduceExecutable createCubePageIndexStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Columnar Page Index");
        result.setMapReduceJobClass(ParquetPageIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Columnar_Page_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getCubeFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getCubePageIndexTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public UpdateOutputDirStep createCubePageIndexCleanupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getCubeFolderPath(seg));
        result.setSubdirFilter(CubePageIndexTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Cube Index Output");
        return result;
    }

    public MapReduceExecutable createRawtablePageIndexStep(String jobId) {
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
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getRawTableFolderPath(rawSeg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawtablePageIndexTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public UpdateOutputDirStep createRawtableFuzzyIndexCleanupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getRawTableFolderPath(seg));
        result.setSubdirFilter(RawtableFuzzyIndexTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Rawtable Fuzzy Index Output");
        return result;
    }

    public UpdateOutputDirStep createRawtablePageIndexCleanupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getRawTableFolderPath(seg));
        result.setSubdirFilter(RawtablePageIndexTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Rawtable Index Output");
        return result;
    }

    public UpdateOutputDirStep createRawtableMergeCleanupStep(String jobId, CubeSegment cubeSegment) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getRawTableFolderPath(cubeSegment));
        result.setSubdirFilter(RawtableMergeTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Rawtable Index Output");
        return result;
    }

    public UpdateOutputDirStep createRawtableCleanupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getRawTableFolderPath(seg));
        result.setSubdirFilter(RawtableTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Rawtable Output");
        return result;
    }

    public UpdateOutputDirStep createCubeTarballCleaupStep(String jobId) {
        UpdateOutputDirStep result = new UpdateOutputDirStep();
        result.setOutputDir(getCubeFolderPath(seg));
        result.setSubdirFilter(CubeTarballTmpFolderPrefix);
        result.setJobId(jobId);
        result.setName("Clean Cube Tarball Output");
        return result;
    }

    public ParquetCubeInfoCollectionStep createCubeInfoCollectionStep(String jobId) {
        ParquetCubeInfoCollectionStep step = new ParquetCubeInfoCollectionStep();
        step.setName("Collect Cube File Mapping");
        step.setParam(ParquetCubeInfoCollectionStep.SKIP, "true");
        return step;
    }

    public MapReduceExecutable createRawtableFuzzyIndexStep(String jobId) {
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
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getRawTableFolderPath(rawSeg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getRawtableFuzzyIndexTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public MapReduceExecutable createCubeTarballStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Tarball Columnar Files");
        result.setMapReduceJobClass(ParquetTarballJob.class);
        result.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Tarball_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getCubeFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getCubeTarballTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public List<String> getMergeSegmentsParquetFolders() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> ret = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            ret.add(getCubeFolderPath(merging));
        }
        return ret;
    }

    public List<String> getMergeSegmentJobWorkingDirs() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHDFSPaths.add(getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }

    public void addMergeGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {

        //clean two parts: 1.parquet storage folders 2. working dirs
        List<String> toCleanFolders = getMergeSegmentsParquetFolders();
        toCleanFolders.addAll(getMergeSegmentJobWorkingDirs());

        logger.info("toCleanFolders are :" + toCleanFolders);

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(null);//delete all source folder for merge

        jobFlow.addTask(step);
    }

    public void addCubeGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        List<String> toCleanFolders = Lists.newArrayList(getCubeFolderPath(seg));
        List<String> toCleanFileSuffixs = Lists.newArrayList(".parquet", ".parquet.inv", ".tmp");

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(toCleanFileSuffixs);

        jobFlow.addTask(step);
    }

    public CubeShardSizingStep createCubeShardSizingStep(String jobId) {
        CubeShardSizingStep result = new CubeShardSizingStep();
        result.setName("Sizing Columnar Shards");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    public RawtableShardSizingStep createRawtableShardSizingStep(String jobId) {
        RawtableShardSizingStep result = new RawtableShardSizingStep();
        result.setName("Sizing Raw Shards");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }


    public StorageDuplicateStep createStorageDuplicateStep(String jobId) {
        final StorageDuplicateStep result = new StorageDuplicateStep();

        result.setName("Duplicate Files to Remote Cluster");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    protected String getRawtableFuzzyIndexTmpFolderPath(CubeSegment cubeSegment) {
        return getRawTableFolderPath(cubeSegment) + "/" + RawtableFuzzyIndexTmpFolderPrefix;
    }

    protected String getRawtablePageIndexTmpFolderPath(CubeSegment cubeSegment) {
        return getRawTableFolderPath(cubeSegment) + "/" + RawtablePageIndexTmpFolderPrefix;
    }

    protected String getRawtableTmpFolderPath(CubeSegment cubeSegment) {
        return getRawTableFolderPath(cubeSegment) + "/" + RawtableTmpFolderPrefix;
    }

    protected String getRawtableMergeTmpFolderPath(CubeSegment cubeSegment) {
        return getRawTableFolderPath(cubeSegment) + "/" + RawtableMergeTmpFolderPrefix;
    }

    protected String getCubePageIndexTmpFolderPath(CubeSegment cubeSegment) {
        return getCubeFolderPath(cubeSegment) + "/" + CubePageIndexTmpFolderPrefix;
    }

    protected String getCubeTarballTmpFolderPath(CubeSegment cubeSegment) {
        return getCubeFolderPath(cubeSegment) + "/" + CubeTarballTmpFolderPrefix;
    }

    protected String getCubeMergeTmpFolderPath(CubeSegment cubeSegment) {
        return getCubeFolderPath(cubeSegment) + "/" + CubeMergeTmpFolderPrefix;
    }

    protected String getRawTableFolderPath(CubeSegment cubeSegment) {
        RawTableInstance instance = detectRawTable(cubeSegment);
        RawTableSegment rawSeg = instance.getSegmentById(cubeSegment.getUuid());
        return ColumnarStorageUtils.getSegmentDir(instance, rawSeg);
    }

    protected String getRawTableFolderPath(RawTableSegment rawSegment) {
        return ColumnarStorageUtils.getSegmentDir(rawSegment.getRawTableInstance(), rawSegment);
    }

    protected String getCubeFolderPath(CubeSegment cubeSegment) {
        return ColumnarStorageUtils.getSegmentDir(cubeSegment.getCubeInstance(), cubeSegment);
    }

    protected RawTableInstance detectRawTable(CubeSegment cubeSegment) {
        RawTableInstance rawInstance = RawTableManager.getInstance(cubeSegment.getConfig()).getRawTableInstance(cubeSegment.getRealization().getName());
        logger.info("Raw table is " + (rawInstance == null ? "not " : "") + "specified in this cubing job " + cubeSegment);
        return rawInstance;
    }
}
