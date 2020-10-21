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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.rest.util.AclPermissionUtil.isAdmin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.project.NProjectManager;

@Component("asyncQueryService")
public class AsyncQueryService extends QueryService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryService.class);

    public enum QueryStatus {
        RUNNING, FAILED, SUCCESS, MISS
    }

    protected FileSystem getFileSystem() {
        return HadoopUtil.getWorkingFileSystem();
    }

    public void saveUserName(String project, String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getUserFileName()));
                OutputStreamWriter osw = new OutputStreamWriter(os)) {
            osw.write(getUsername());
        }
    }

    public void saveMetaData(String project, SQLResponse sqlResponse, String queryId) throws IOException {
        ArrayList<String> dataTypes = Lists.newArrayList();
        ArrayList<String> columnNames = Lists.newArrayList();
        for (SelectedColumnMeta selectedColumnMeta : sqlResponse.getColumnMetas()) {
            dataTypes.add(selectedColumnMeta.getColumnTypeName());
            columnNames.add(selectedColumnMeta.getName());
        }

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os)) {
            String metaString = Strings.join(columnNames, ",") + "\n" + Strings.join(dataTypes, ",");
            osw.write(metaString);

        }
    }

    public List<List<String>> getMetaData(String project, String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        List<List<String>> result = Lists.newArrayList();
        FileSystem fileSystem = getFileSystem();
        try (FSDataInputStream is = fileSystem.open(new Path(asyncQueryResultDir, getMetaDataFileName()));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is))) {
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
        }
        return result;
    }

    public void createErrorFlag(String project, String queryId, String errorMessage) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getFailureFlagFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os)) {
            if (errorMessage != null) {
                osw.write(errorMessage);
                os.hflush();
            }
        }
    }

    public void retrieveSavedQueryResult(String project, String queryId, boolean includeHeader,
            HttpServletResponse response) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());

        FileSystem fileSystem = getFileSystem();
        Path dataPath = getAsyncQueryResultDir(project, queryId);

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(MsgPicker.getMsg().getQUERY_RESULT_FILE_NOT_FOUND());
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        ServletOutputStream outputStream = response.getOutputStream();
        try {
            if (includeHeader) {
                String columns = null;
                for (FileStatus header : fileStatuses) {
                    if (header.getPath().getName().equals(getMetaDataFileName())) {
                        try (FSDataInputStream inputStream = fileSystem.open(header.getPath());
                                BufferedReader bufferedReader = new BufferedReader(
                                        new InputStreamReader(inputStream))) {
                            columns = bufferedReader.readLine();
                            break;
                        }
                    }
                }

                if (columns != null) {
                    logger.debug("Query:{}, columnMeta:{}", queryId, columns);
                    if (!columns.endsWith(IOUtils.LINE_SEPARATOR_UNIX)) {
                        columns = columns + IOUtils.LINE_SEPARATOR_UNIX;
                    }
                    IOUtils.copy(IOUtils.toInputStream(columns), outputStream);
                } else {
                    logger.error("Query:{}, no columnMeta found", queryId);
                }
            }
            for (FileStatus f : fileStatuses) {
                if (!f.getPath().getName().startsWith("_")) {
                    try (FSDataInputStream inputStream = fileSystem.open(f.getPath())) {
                        IOUtils.copyLarge(inputStream, outputStream);
                    }
                }
            }
        } finally {
            outputStream.close();
        }
    }

    public String retrieveSavedQueryException(String project, String queryId) throws IOException {
        Message msg = MsgPicker.getMsg();

        FileSystem fileSystem = getFileSystem();
        Path dataPath = new Path(getAsyncQueryResultDir(project, queryId), getFailureFlagFileName());

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(msg.getQUERY_EXCEPTION_FILE_NOT_FOUND());
        }
        try (FSDataInputStream inputStream = fileSystem.open(dataPath);
                InputStreamReader reader = new InputStreamReader(inputStream)) {
            List<String> strings = IOUtils.readLines(reader);

            return StringUtils.join(strings, "");
        }
    }

    public QueryStatus queryStatus(String project, String queryId) throws IOException {
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (getFileSystem().exists(asyncQueryResultDir)) {
            if (getFileSystem().exists(new Path(asyncQueryResultDir, getFailureFlagFileName()))) {
                return QueryStatus.FAILED;
            }
            if (getFileSystem().exists(new Path(asyncQueryResultDir, getSuccessFlagFileName()))) {
                return QueryStatus.SUCCESS;
            }
            return QueryStatus.RUNNING;
        }
        return QueryStatus.MISS;
    }

    public String getUserName(String queryId, String project) throws IOException {
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        FileSystem fileSystem = getFileSystem();
        if (fileSystem.exists(asyncQueryResultDir)) {
            try (FSDataInputStream is = fileSystem.open(new Path(asyncQueryResultDir, getUserFileName()));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is))) {
                return bufferedReader.readLine();
            }
        }
        return null;
    }

    public boolean hasPermission(String queryId, String project) throws IOException {
        if (getUserName(queryId, project) != null && SecurityContextHolder.getContext().getAuthentication().getName()
                .equals(getUserName(queryId, project))) {
            return true;
        }
        if (isAdmin()) {
            return true;
        }
        return false;
    }

    public long fileStatus(String project, String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (getFileSystem().exists(asyncQueryResultDir) && getFileSystem().isDirectory(asyncQueryResultDir)) {
            long totalFileSize = 0;
            FileStatus[] fileStatuses = getFileSystem().listStatus(asyncQueryResultDir);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().startsWith("_"))
                    totalFileSize += fileStatus.getLen();
            }
            return totalFileSize;
        } else {
            throw new BadRequestException(MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        }
    }

    public boolean batchDelete(String project, String time) throws Exception {
        if (project == null && time == null) {
            return deleteAllFolder();
        } else {
            return deleteOldQueryResult(project, time);
        }
    }

    public boolean deleteAllFolder() throws IOException {
        NProjectManager projectManager = KylinConfig.getInstanceFromEnv().getManager(NProjectManager.class);
        List<ProjectInstance> projectInstances = projectManager.listAllProjects();
        boolean allSuccess = true;
        Set<Path> asyncQueryResultPaths = new HashSet<Path>();
        FileSystem fileSystem = getFileSystem();
        for (ProjectInstance projectInstance : projectInstances) {
            Path asyncQueryResultBaseDir = getAsyncQueryResultBaseDir(projectInstance.getName());
            if (!fileSystem.exists(asyncQueryResultBaseDir)) {
                continue;
            }
            asyncQueryResultPaths.add(asyncQueryResultBaseDir);
        }

        logger.info("clean all async result dir");
        for (Path path : asyncQueryResultPaths) {
            if (!getFileSystem().delete(path, true)) {
                allSuccess = false;
            }
        }
        return allSuccess;
    }

    public boolean deleteByQueryId(String project, String queryId) throws IOException {
        Path resultDir = getAsyncQueryResultDir(project, queryId);
        if (!getFileSystem().exists(resultDir)) {
            return true;
        }
        logger.info("clean async query result for query id [{}]", queryId);
        return getFileSystem().delete(resultDir, true);
    }

    public boolean deleteOldQueryResult(String project, long time) throws IOException {
        boolean isAllSucceed = true;
        Path asyncQueryResultBaseDir = getAsyncQueryResultBaseDir(project);
        FileSystem fileSystem = getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(asyncQueryResultBaseDir);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.getModificationTime() < time) {
                try {
                    fileSystem.delete(fileStatus.getPath(), true);
                } catch (Exception e) {
                    logger.error("Fail to delete async query result for [{}]", fileStatus.getPath(), e);
                    isAllSucceed = false;
                }
            }
        }
        return isAllSucceed;
    }

    public boolean deleteOldQueryResult(String project, String timeString) throws IOException, ParseException {
        if (project == null || timeString == null) {
            return false;
        }
        long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeString).getTime();
        return deleteOldQueryResult(project, time);
    }

    public boolean cleanOldQueryResult(String project, long days) throws IOException {
        return deleteOldQueryResult(project, System.currentTimeMillis() - days * 24 * 60 * 60 * 1000);
    }

    public String asyncQueryResultPath(String project, String queryId) throws IOException {
        if (queryStatus(project, queryId) == QueryStatus.MISS) {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        }
        return getAsyncQueryResultDir(project, queryId).toString();
    }

    public void checkStatus(String queryId, QueryStatus queryStatus, String project, String message)
            throws IOException {
        switch (queryStatus) {
        case SUCCESS:
            if (queryStatus(project, queryId) != QueryStatus.SUCCESS) {
                throw new NAsyncQueryIllegalParamException(message);
            }
            break;
        default:
            break;
        }
    }

    public Path getAsyncQueryResultBaseDir(String project) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(project));
    }

    public Path getAsyncQueryResultDir(String project, String queryId) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(project), queryId);
    }

    public String getSuccessFlagFileName() {
        return "_SUCCESS";
    }

    public String getFailureFlagFileName() {
        return "_FAILED";
    }

    public String getMetaDataFileName() {
        return "_METADATA";
    }

    public String getUserFileName() {
        return "_USER";
    }
}
