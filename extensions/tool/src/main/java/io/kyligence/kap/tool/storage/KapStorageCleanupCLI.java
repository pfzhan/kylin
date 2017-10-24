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

package io.kyligence.kap.tool.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.rest.job.StorageCleanupJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;

// TODO: Should consider read/write separate deployment
public class KapStorageCleanupCLI extends StorageCleanupJob {

    protected static final Logger logger = LoggerFactory.getLogger(KapStorageCleanupCLI.class);

    public static void main(String[] args) throws Exception {
        KapStorageCleanupCLI cli = new KapStorageCleanupCLI();
        cli.execute(args);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        super.execute(optionsHelper);
        Configuration conf = new Configuration();
        cleanUnusedParquetFolders(conf);
        cleanUnusedIntermediateHDFSFile(conf);
    }

    @Override
    protected void cleanUnusedHBaseTables() throws IOException {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        if ("hbase".equals(metadataUrl.getScheme())) {
            super.cleanUnusedHBaseTables();
        } else {
            logger.info("Not using HBase as metadata resource store, skip hbase clean up job.");
        }
    }

    private List<String> getNeedToDeletePath(Configuration conf, String rootDir) throws IOException {
        logger.info("The root path is: {}", rootDir);
        List<String> deleteList = new ArrayList<>();
        FileSystem fs = HadoopUtil.getWorkingFileSystem(conf);
        FileStatus[] rootFolders;

        Path rootPath = new Path(rootDir);

        if ("/tmp".equals(rootDir) && fs.exists(rootPath)) {
            rootPath = fs.makeQualified(rootPath);
            rootFolders = fs.listStatus(rootPath);
            if (rootFolders != null) {
                for (FileStatus statsFolder : rootFolders) {
                    String name = statsFolder.getPath().getName();
                    if (name.startsWith("migrating_cube_"))
                        deleteList.add(rootPath + File.separator + name);
                }
            }
            return deleteList;
        }

        if (fs.exists(rootPath)) {
            rootFolders = fs.listStatus(rootPath);
            if (rootFolders != null) {
                for (FileStatus statsFolder : rootFolders) {
                    try {
                        String jobId = statsFolder.getPath().getName();
                        final ExecutableState state = executableManager.getOutput(jobId).getState();
                        if (!state.isFinalState()) {
                            continue;
                        }
                        String folderName = rootPath + File.separator + jobId;
                        deleteList.add(folderName);
                    } catch (Exception e) {
                        logger.error("Ignore executableManager.getOutput exception: {}", e);
                    }
                }
            }
        }
        return deleteList;
    }

    private void cleanUnusedIntermediateHDFSFile(Configuration conf) throws IOException {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        List<String> allHDFSPathsNeedToBeDeleted = new ArrayList<>();

        String modelStatsStoragePath = kapConfig.getWriteHdfsWorkingDirectory() + "model_stats";
        String tableStatsStoragePath = kapConfig.getWriteHdfsWorkingDirectory() + "table_stats";
        allHDFSPathsNeedToBeDeleted.addAll(getNeedToDeletePath(conf, modelStatsStoragePath));
        allHDFSPathsNeedToBeDeleted.addAll(getNeedToDeletePath(conf, tableStatsStoragePath));
        allHDFSPathsNeedToBeDeleted.addAll(getNeedToDeletePath(conf, "/tmp"));


        if (delete) {
            FileSystem fs = HadoopUtil.getWorkingFileSystem(conf);
            for (String hdfsPath : allHDFSPathsNeedToBeDeleted) {
                logger.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p)) {
                    fs.delete(p, true);
                    logger.info("Deleted HDFS path " + hdfsPath);
                } else {
                    logger.info("HDFS path " + hdfsPath + "does not exist");
                }
            }

        } else {
            System.out.println("--------------- Intermediate Stats HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHDFSPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }
    }

    private void cleanUnusedParquetFolders(Configuration conf) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = HadoopUtil.getWorkingFileSystem(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        Path parquetStoragePath = new Path(KapConfig.getInstanceFromEnv().getReadParquetStoragePath());
        FileStatus[] realizationParquetFolders = null;
        if (fs.exists(parquetStoragePath)) {
            realizationParquetFolders = fs.listStatus(parquetStoragePath);
            if (realizationParquetFolders != null) {
                for (FileStatus dataFolder : realizationParquetFolders) { //folders for cubes
                    FileStatus[] segmentFolders = fs.listStatus(dataFolder.getPath());

                    for (FileStatus segmentFolder : segmentFolders) {
                        String folderName = KapConfig.getInstanceFromEnv().getReadParquetStoragePath()
                                + dataFolder.getPath().getName() + "/" + segmentFolder.getPath().getName();
                        allHdfsPathsNeedToBeDeleted.add(folderName);
                    }
                }
            }
        }

        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                try {
                    Map<String, String> params = executableManager.getJob(jobId).getParams();
                    String cubeName = CubingExecutableUtil.getCubeName(params);

                    if (cubeName == null) {
                        //skip job like "calculate cardinality"
                        continue;
                    }

                    String segmentId = CubingExecutableUtil.getSegmentId(params);

                    if (cubeMgr.getCube(cubeName) != null) {
                        String cubeId = cubeMgr.getCube(cubeName).getId();
                        String cubePath = KapConfig.getInstanceFromEnv().getReadParquetStoragePath() + cubeId + "/"
                                + segmentId;
                        allHdfsPathsNeedToBeDeleted.remove(cubePath);
                        logger.info("Skip " + cubePath + " from deletion list, as the path belongs to job " + jobId
                                + " with state " + state);
                    }

                    if (rawMgr.getRawTableInstance(cubeName) != null) {
                        String rawId = rawMgr.getRawTableInstance(cubeName).getId();
                        String rawPath = KapConfig.getInstanceFromEnv().getReadParquetStoragePath() + rawId + "/"
                                + segmentId;
                        allHdfsPathsNeedToBeDeleted.remove(rawPath);
                        logger.info("Skip " + rawPath + " from deletion list, as the path belongs to job " + jobId
                                + " with state " + state);
                    }
                } catch (Exception ex) {
                    logger.warn("Failed to find segment ID from job ID " + jobId + ", ignore it");
                    // some older version job metadata may fail to read, ignore it
                }
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                SegmentStatusEnum status = seg.getStatus();

                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String exclude = KapConfig.getInstanceFromEnv().getReadParquetStoragePath() + cube.getId() + "/"
                            + seg.getUuid();
                    allHdfsPathsNeedToBeDeleted.remove(exclude);
                    logger.info("Skip " + exclude + " from deletion list, as the path belongs to segment " + seg
                            + " of cube " + cube.getName() + ", with status " + status);
                }
            }
        }

        // remove every rawtable segment working dir from deletion list
        for (RawTableInstance raw : rawMgr.listAllRawTables()) {
            for (RawTableSegment seg : raw.getSegments()) {
                SegmentStatusEnum status = seg.getStatus();
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String exclude = KapConfig.getInstanceFromEnv().getWriteParquetStoragePath() + raw.getId() + "/"
                            + seg.getUuid();
                    allHdfsPathsNeedToBeDeleted.remove(exclude);
                    logger.info("Skip " + exclude + " from deletion list, as the path belongs to segment " + seg
                            + " of rawtable " + raw.getName() + ", with status " + status);
                }
            }
        }

        if (delete) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                logger.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p)) {
                    fs.delete(p, true);
                    logger.info("Deleted HDFS path " + hdfsPath);
                } else {
                    logger.info("HDFS path " + hdfsPath + "does not exist");
                }
            }

            if (realizationParquetFolders != null) {
                for (FileStatus dataFolder : realizationParquetFolders) { //folders for cubes
                    FileStatus[] segmentFolders = fs.listStatus(dataFolder.getPath());
                    if (segmentFolders == null || segmentFolders.length == 0) {
                        logger.info("Cleaning empty realization folder: " + dataFolder.getPath());
                        fs.delete(dataFolder.getPath(), true);
                    }
                }
            }
        } else {
            System.out.println("--------------- HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }

    }

}
