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
package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ParquetStorageCleanupStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(ParquetStorageCleanupStep.class);

    public static final String TO_CLEAN_FOLDERS = "toCleanFolders";
    public static final String TO_CLEAN_FILE_SUFFIX = "toCleanFileSuffix";
    private StringBuffer output;

    public ParquetStorageCleanupStep() {
        super();
        output = new StringBuffer();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            List<String> toCleanPaths = getToCleanFolders();
            List<String> toCleanPatterns = getToCleanFileSuffix();
            dropHdfsPathOnCluster(toCleanPaths, toCleanPatterns, FileSystem.get(HadoopUtil.getCurrentConfiguration()));

        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            output.append("\n").append(e.getLocalizedMessage());
            return new ExecuteResult(ExecuteResult.State.ERROR, output.toString());
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
    }

    private boolean matchFileSuffix(String fileName, List<String> fileSuffixs) {
        for (String suffix : fileSuffixs) {
            if (fileName.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private void dropHdfsPathOnCluster(List<String> folderPaths, List<String> fileSuffixs, FileSystem fileSystem) throws IOException {
        logger.info("folderPaths is {}", folderPaths);
        logger.info("fileSuffixs is {}", fileSuffixs);
        if (folderPaths != null && folderPaths.size() > 0) {
            logger.debug("Drop HDFS path on FileSystem: " + fileSystem.getUri());
            output.append("Drop HDFS path on FileSystem: \"" + fileSystem.getUri() + "\" \n");
            for (String folder : folderPaths) {
                Path folderPath = new Path(folder);
                if (fileSystem.exists(folderPath) && fileSystem.isDirectory(folderPath)) {
                    if (fileSuffixs != null && fileSuffixs.size() > 0) {
                        logger.info("Selectively delete some files");
                        dropCuboidPath(fileSystem, folderPath, fileSuffixs);
                    } else {
                        logger.info("Delete entire folders");
                        //if no file suffix provided, delete the whole folder
                        logger.debug("working on HDFS folder " + folder);
                        output.append("working on HDFS folder " + folder + "\n");
                        fileSystem.delete(folderPath, true);
                        logger.debug("Successfully dropped.");
                        output.append("Successfully dropped.\n");
                    }
                } else {
                    logger.debug("Folder " + folder + " not exists.");
                    output.append("Folder " + folder + " not exists.\n");
                }
            }
        }
    }

    private void dropCuboidPath(FileSystem fs, Path path, List<String> fileSuffixs) throws IOException {
        if (!fs.exists(path)) {
            logger.warn("path {} does not exist", path.toString());
        } else if (fs.isDirectory(path)) {
            for (FileStatus fileStatus : fs.listStatus(path)) {
                // only delete cuboid files
                if (fileStatus.getPath().getName().matches("^\\d+$")) {
                    dropPath(fs, fileStatus.getPath(), fileSuffixs);
                }
            }
        } else {
            logger.warn("path {} should be folder", path.toString());
        }
    }

    private void dropPath(FileSystem fs, Path path, List<String> fileSuffixs) throws IOException {
        if (!fs.exists(path)) {
            logger.warn("path {} does not exist", path.toString());
        } else if (fs.isDirectory(path)) {
            for (FileStatus fileStatus : fs.listStatus(path)) {
                dropPath(fs, fileStatus.getPath(), fileSuffixs);
            }
        } else {
            String currentFileName = path.toString();
            if (matchFileSuffix(currentFileName, fileSuffixs)) {
                long size = fs.getContentSummary(path).getLength();
                logger.debug("working on HDFS file " + currentFileName + " with size " + size);
                output.append("working on HDFS file " + currentFileName + " with size " + size + "\n");
                fs.delete(path, false);
                logger.debug("Successfully deleted.");
                output.append("Successfully deleted.\n");
            }
        }
    }

    public void setToCleanFolders(List<String> deletePaths) {
        setArrayParam(TO_CLEAN_FOLDERS, deletePaths);
    }

    public List<String> getToCleanFolders() {
        return getArrayParam(TO_CLEAN_FOLDERS);
    }

    public void setToCleanFileSuffix(List<String> deletePaths) {
        setArrayParam(TO_CLEAN_FILE_SUFFIX, deletePaths);
    }

    public List<String> getToCleanFileSuffix() {
        return getArrayParam(TO_CLEAN_FILE_SUFFIX);
    }

    private void setArrayParam(String paramKey, List<String> paramValues) {
        setParam(paramKey, StringUtils.join(paramValues, ","));
    }

    private List<String> getArrayParam(String paramKey) {
        final String ids = getParam(paramKey);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }
}
