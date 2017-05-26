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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
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
            dropHdfsPathOnCluster(toCleanPaths, toCleanPatterns, HadoopUtil.getWorkingFileSystem());

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
        if (folderPaths != null && folderPaths.size() > 0) {
            logger.info("Drop HDFS path on FileSystem: " + fileSystem.getUri());
            output.append("Drop HDFS path on FileSystem: \"" + fileSystem.getUri() + "\" \n");
            for (String folder : folderPaths) {
                Path folderPath = new Path(folder);
                if (fileSystem.exists(folderPath) && fileSystem.isDirectory(folderPath)) {
                    if (fileSuffixs != null && fileSuffixs.size() > 0) {
                        logger.info("Selectively delete some files: folderPath={}, fileSuffix={}", folderPaths, fileSuffixs);
                        dropCuboidPath(fileSystem, folderPath, fileSuffixs);
                    } else {
                        logger.info("Delete entire folders.");
                        //if no file suffix provided, delete the whole folder
                        logger.debug("working on HDFS folder " + folder);
                        output.append("working on HDFS folder " + folder + "\n");
                        fileSystem.delete(folderPath, true);
                    }
                } else {
                    logger.warn("Folder " + folder + " not exists.");
                    output.append("Folder " + folder + " not exists.\n");
                }
            }
        }
    }

    protected void dropCuboidPath(FileSystem fs, Path path, List<String> fileSuffixs) throws IOException {
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

    protected void dropPath(FileSystem fs, Path path, List<String> fileSuffixs) throws IOException {
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
                output.append("working on HDFS file " + currentFileName + " with size " + size + "\n");
                fs.delete(path, false);
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
