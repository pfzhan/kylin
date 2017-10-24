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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

@Component("asyncQueryService")
public class AsyncQueryService extends QueryService {

    public static final String BASE_FOLDER = "async_query_result";
    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryService.class);

    protected FileSystem getFileSystem() throws IOException {
        return HadoopUtil.getWorkingFileSystem();
    }

    public void createExistFlag(String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        fileSystem.mkdirs(asyncQueryResultDir);
        Path outputPath = new Path(asyncQueryResultDir, getExistFlagFileName(queryId));
        fileSystem.createNewFile(outputPath);
    }

    public void flushResultToHdfs(SQLResponse result, String queryId) throws IOException {
        logger.info("Flushing results to hdfs...");
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        fileSystem.mkdirs(asyncQueryResultDir);
        Path outputPath = new Path(asyncQueryResultDir, queryId);

        if (result.getIsException()) {
            try (FSDataOutputStream os = fileSystem.create(outputPath); //
                    OutputStreamWriter osw = new OutputStreamWriter(os)) {
                osw.write(result.getExceptionMessage());
            }

            Path failureFlagPath = new Path(asyncQueryResultDir, getFailureFlagFileName(queryId));
            fileSystem.createNewFile(failureFlagPath);
            logger.info("failed result flushed");
        } else {
            try (FSDataOutputStream os = fileSystem.create(outputPath); //
                    OutputStreamWriter osw = new OutputStreamWriter(os); //
                    ICsvListWriter csvWriter = new CsvListWriter(osw, CsvPreference.STANDARD_PREFERENCE)) {

                List<String> headerList = new ArrayList<String>();

                for (SelectedColumnMeta column : result.getColumnMetas()) {
                    headerList.add(column.getName());
                }

                String[] headers = new String[headerList.size()];
                csvWriter.writeHeader(headerList.toArray(headers));

                for (List<String> row : result.getResults()) {
                    csvWriter.write(row);
                }
            }

            Path successFlagPath = new Path(asyncQueryResultDir, getSuccessFlagFileName(queryId));
            fileSystem.createNewFile(successFlagPath);
            logger.info("successful result flushed");
        }

    }

    public void retrieveSavedQueryResult(String queryId, HttpServletResponse response) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!isQuerySuccessful(queryId)) {
            throw new BadRequestException(msg.getQUERY_RESULT_NOT_FOUND());
        }

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        Path dataPath = new Path(asyncQueryResultDir, queryId);

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(msg.getQUERY_RESULT_FILE_NOT_FOUND());
        }

        try (FSDataInputStream inputStream = fileSystem.open(dataPath); ServletOutputStream outputStream = response.getOutputStream()) {
            IOUtils.copyLarge(inputStream, outputStream);
        }
    }

    public String retrieveSavedQueryException(String queryId) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (!isQueryFailed(queryId)) {
            throw new BadRequestException(msg.getQUERY_EXCEPTION_NOT_FOUND());
        }

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        Path dataPath = new Path(asyncQueryResultDir, queryId);

        if (!fileSystem.exists(dataPath)) {
            throw new BadRequestException(msg.getQUERY_EXCEPTION_FILE_NOT_FOUND());
        }

        try (FSDataInputStream inputStream = fileSystem.open(dataPath); InputStreamReader reader = new InputStreamReader(inputStream)) {
            List<String> strings = IOUtils.readLines(reader);

            return StringUtils.join(strings, "");
        }
    }

    public boolean cleanFolder() throws IOException {
        return getFileSystem().delete(getAsyncQueryResultDir(), true);
    }

    public boolean isQueryExisting(String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        Path initFlagPath = new Path(asyncQueryResultDir, getExistFlagFileName(queryId));
        return fileSystem.exists(initFlagPath);
    }

    public boolean isQuerySuccessful(String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        Path successFlagPath = new Path(asyncQueryResultDir, getSuccessFlagFileName(queryId));
        return fileSystem.exists(successFlagPath);
    }

    public boolean isQueryFailed(String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir();
        Path failureFlagPath = new Path(asyncQueryResultDir, getFailureFlagFileName(queryId));
        return fileSystem.exists(failureFlagPath);
    }

    protected Path getAsyncQueryResultDir() {
        String hdfsWorkingDirectory = KapConfig.getInstanceFromEnv().getReadHdfsWorkingDirectory();
        Path asyncQueryResultDir = new Path(hdfsWorkingDirectory, BASE_FOLDER);
        return asyncQueryResultDir;
    }

    private String getExistFlagFileName(String queryId) {
        return queryId + ".exist";
    }

    private String getSuccessFlagFileName(String queryId) {
        return queryId + ".success";
    }

    private String getFailureFlagFileName(String queryId) {
        return queryId + ".failure";
    }
}
