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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

@Component("asyncQueryService")
public class AsyncQueryService extends QueryService {

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryService.class);

    public enum QueryStatus {
        RUNNING, FAILED, SUCCESS, MISS
    }

    private Cache<String, QueryStatus> queryStatusCache = CacheBuilder.newBuilder().maximumSize(1000)
            .expireAfterWrite(1, TimeUnit.DAYS).build();

    protected FileSystem getFileSystem() throws IOException {
        return HadoopUtil.getWorkingFileSystem();
    }

    public void updateStatus(String queryId, QueryStatus status) {
        queryStatusCache.put(queryId, status);
    }

    public void saveMetaData(SQLResponse sqlResponse, String queryId) throws IOException {
        ArrayList<String> dataTypes = Lists.newArrayList();
        ArrayList<String> columnNames = Lists.newArrayList();
        for (SelectedColumnMeta selectedColumnMeta : sqlResponse.getColumnMetas()) {
            dataTypes.add(selectedColumnMeta.getColumnTypeName());
            columnNames.add(selectedColumnMeta.getName());
        }

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(queryId);
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os)) {
            String metaString = Strings.join(columnNames, ",") + "\n" + Strings.join(dataTypes, ",");
            osw.write(metaString);

        }
    }

    public List<List<String>> getMetaData(String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, KapMsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(queryId);
        List<List<String>> result = Lists.newArrayList();
        try (FSDataInputStream is = fileSystem.open(new Path(asyncQueryResultDir, getMetaDataFileName()));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is))) {
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
        }
        //
        return result;

    }

    public void createErrorFlag(String errorMessage, String queryId) throws IOException {
        updateStatus(queryId, AsyncQueryService.QueryStatus.FAILED);
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getFailureFlagFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os)) {
            osw.write(errorMessage);
            os.hflush();
        }

    }

    public void retrieveSavedQueryResult(String queryId, HttpServletResponse response) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, KapMsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultBaseDir();
        Path dataPath = new Path(asyncQueryResultDir, queryId);

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(KapMsgPicker.getMsg().getQUERY_RESULT_FILE_NOT_FOUND());
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        ServletOutputStream outputStream = response.getOutputStream();
        try {
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

    public String retrieveSavedQueryException(String queryId) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        checkStatus(queryId, QueryStatus.FAILED, msg.getQUERY_EXCEPTION_FILE_NOT_FOUND());

        FileSystem fileSystem = getFileSystem();
        Path dataPath = new Path(getAsyncQueryResultDir(queryId), getFailureFlagFileName());

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(msg.getQUERY_EXCEPTION_FILE_NOT_FOUND());
        }
        try (FSDataInputStream inputStream = fileSystem.open(dataPath);
                InputStreamReader reader = new InputStreamReader(inputStream)) {
            List<String> strings = IOUtils.readLines(reader);

            return StringUtils.join(strings, "");
        }
    }

    public QueryStatus queryStatus(String queryId) throws IOException {
        QueryStatus ifPresent = queryStatusCache.getIfPresent(queryId);
        if (ifPresent != null) {
            return ifPresent;
        }
        Path asyncQueryResultDir = getAsyncQueryResultDir(queryId);
        if (getFileSystem().exists(asyncQueryResultDir)) {
            if (getFileSystem().exists(new Path(asyncQueryResultDir, getFailureFlagFileName()))) {
                return QueryStatus.FAILED;
            }
            if (getFileSystem().exists(new Path(asyncQueryResultDir, getSuccessFlagFileName()))) {
                return QueryStatus.SUCCESS;
            }
        }
        return QueryStatus.MISS;
    }

    public long fileStatus(String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, KapMsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        Path asyncQueryResultDir = getAsyncQueryResultDir(queryId);
        if (getFileSystem().exists(asyncQueryResultDir) && getFileSystem().isDirectory(asyncQueryResultDir)) {
            long totalFileSize = 0;
            FileStatus[] fileStatuses = getFileSystem().listStatus(asyncQueryResultDir);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().startsWith("_"))
                    totalFileSize += fileStatus.getLen();
            }
            return totalFileSize;
        } else {
            throw new BadRequestException(KapMsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        }
    }


    public boolean cleanAllFolder() throws IOException {
        Path asyncQueryResultBaseDir = getAsyncQueryResultBaseDir();
        if (!getFileSystem().exists(asyncQueryResultBaseDir)) {
            return true;
        }
        logger.info("clean all async result dir");
        return getFileSystem().delete(asyncQueryResultBaseDir, true);
    }

    public String asyncQueryResultPath(String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, KapMsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        return getAsyncQueryResultDir(queryId).toString();

    }

    public void checkStatus(String queryId, QueryStatus queryStatus, String message) throws IOException {
        switch (queryStatus) {
        case SUCCESS:
            if (queryStatus(queryId) != QueryStatus.SUCCESS) {
                throw new BadRequestException(message);
            }
            break;
        case RUNNING:
            if (queryStatus(queryId) != QueryStatus.RUNNING) {
                throw new BadRequestException(message);
            }
            break;
        case FAILED:
            if (queryStatus(queryId) != QueryStatus.FAILED) {
                throw new BadRequestException(message);
            }
            break;
        case MISS:
            if (queryStatus(queryId) != QueryStatus.MISS) {
                throw new BadRequestException(message);
            }
            break;
        default:
            break;
        }
    }

    public Path getAsyncQueryResultBaseDir() {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir());
    }

    public Path getAsyncQueryResultDir(String queryId) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(), queryId);
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
}
