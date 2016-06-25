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

package io.kyligence.kap.engine.mr.steps;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.SegmentIndexMerge;

public class MergeSecondaryIndexStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(MergeSecondaryIndexStep.class);

    public MergeSecondaryIndexStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        final String indexPath = CubingExecutableUtil.getIndexPath(this.getParams());

        Collections.sort(mergingSegments);

        final int rowKeyNum = newSegment.getCubeDesc().getRowkey().getRowKeyColumns().length;
        final int[] columnsNeedIndex = newSegment.getCubeDesc().getRowkey().getColumnsNeedIndex();

        if (columnsNeedIndex.length == 0) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "Skipped, no index to merge");
        }

        if (columnsNeedIndex.length != (columnsNeedIndex[columnsNeedIndex.length - 1] + 1)) {
            return new ExecuteResult(ExecuteResult.State.ERROR, "Index columns are not continuous from head, couldn't merge index files.");
        }

        try {

            //1. download index files to local
            File localTempFolder = downloadToLocal(mergingSegments);
            localTempFolder.deleteOnExit();

            //2. merge
            SegmentIndexMerge segmentIndexMerge = new SegmentIndexMerge(newSegment, mergingSegments, localTempFolder);

            List<File> newIndexFiles = segmentIndexMerge.mergeIndex();

            //3. upload new index files to hdfs
            uploadToHdfs(indexPath, newIndexFiles);

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to merge secondary index files", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private File downloadToLocal(List<CubeSegment> mergingSegments) throws IOException {
        logger.info("downloading index files to local for merge");
        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());

        File localFolder = new File(File.createTempFile("tmp", null).getParent());
        logger.info("use local folder '" + localFolder.getAbsolutePath() + "'");
        for (CubeSegment seg : mergingSegments) {
            File folderForSeg = new File(localFolder, seg.getName());
            folderForSeg.mkdirs();
            FileStatus[] statuses = fs.listStatus(new Path(seg.getIndexPath()));
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i].isFile()) {
                    logger.info("copyToLocal: " + statuses[i].getPath());
                    fs.copyToLocalFile(false, statuses[i].getPath(), new Path(folderForSeg.toURI()));
                }
            }
        }

        logger.info("downloading finished.");
        return localFolder;
    }

    private void uploadToHdfs(String hdfsPath, List<File> newIndexFiles) throws IOException {

        // upload to hdfs
        try (FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration())) {
            Path path = new Path(hdfsPath);
            fs.mkdirs(path);
            for (File f : newIndexFiles) {
                fs.copyFromLocalFile(true, new Path(f.toURI()), new Path(path, f.getName()));
                f.delete();
            }
        }

        logger.info("uploading finished.");
    }

}
