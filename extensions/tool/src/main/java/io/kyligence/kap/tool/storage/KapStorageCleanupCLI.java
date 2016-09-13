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

package io.kyligence.kap.tool.storage;

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
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.storage.hbase.util.StorageCleanupJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;

public class KapStorageCleanupCLI extends StorageCleanupJob {

    protected static final Logger logger = LoggerFactory.getLogger(KapStorageCleanupCLI.class);

    public static void main(String[] args) throws Exception {
        KapStorageCleanupCLI cli = new KapStorageCleanupCLI();
        cli.execute(args);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        super.execute(optionsHelper);
        cleanUnusedParquetFolders(new Configuration());
    }

    private void cleanUnusedParquetFolders(Configuration conf) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = FileSystem.get(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        FileStatus[] cubeFolders = fs.listStatus(new Path(KapConfig.getInstanceFromEnv().getParquentStoragePath()));

        for (FileStatus cubeFolder : cubeFolders) { //folders for cubes
            FileStatus[] segmentFolders = fs.listStatus(cubeFolder.getPath());

            for (FileStatus segmentFolder : segmentFolders) {
                String folderName = KapConfig.getInstanceFromEnv().getParquentStoragePath() + cubeFolder.getPath().getName() + "/" + segmentFolder.getPath().getName();
                allHdfsPathsNeedToBeDeleted.add(folderName);
            }
        }

        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                Map<String, String> params = executableManager.getJob(jobId).getParams();
                String cubeName = CubingExecutableUtil.getCubeName(params);
                String segmentId = CubingExecutableUtil.getSegmentId(params);
                String cubeId = cubeMgr.getCube(cubeName).getId();
                String cubePath = KapConfig.getInstanceFromEnv().getParquentStoragePath() + cubeId + "/" + segmentId;
                String rawId = rawMgr.getRawTableInstance(cubeName).getId();
                // TODO: raw segment should have own uuid
                String rawPath = KapConfig.getInstanceFromEnv().getParquentStoragePath() + rawId + "/" + segmentId;
                allHdfsPathsNeedToBeDeleted.remove(cubePath);
                allHdfsPathsNeedToBeDeleted.remove(rawPath);
                logger.info("Skip " + cubePath + " from deletion list, as the path belongs to job " + jobId + " with state " + state);
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                SegmentStatusEnum status = seg.getStatus();

                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String exclude = KapConfig.getInstanceFromEnv().getParquentStoragePath() + cube.getId() + "/" + seg.getUuid();
                    allHdfsPathsNeedToBeDeleted.remove(exclude);
                    logger.info("Skip " + exclude + " from deletion list, as the path belongs to segment " + seg + " of cube " + cube.getName() + ", with status " + status);
                }
            }
        }

        // remove every rawtable segment working dir from deletion list
        for (RawTableInstance raw : rawMgr.listAllRawTables()) {
            for (RawTableSegment seg : raw.getSegments()) {
                SegmentStatusEnum status = seg.getStatus();
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String exclude = KapConfig.getInstanceFromEnv().getParquentStoragePath() + raw.getId() + "/" + seg.getUuid();
                    allHdfsPathsNeedToBeDeleted.remove(exclude);
                    logger.info("Skip " + exclude + " from deletion list, as the path belongs to segment " + seg + " of rawtable " + raw.getName() + ", with status " + status);
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
                    logger.info("Deleted hdfs path " + hdfsPath);
                } else {
                    logger.info("Hdfs path " + hdfsPath + "does not exist");
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
