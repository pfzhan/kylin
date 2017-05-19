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

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.rest.exception.BadRequestException;
import org.springframework.stereotype.Component;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by luwei on 17-4-29.
 */
@Component("asyncQueryServiceV2")
public class AsyncQueryServiceV2 extends AsyncQueryService {

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

            //            //cleaning
            //            cleanFiles(fileSystem, asyncQueryResultDir, queryId);

            return StringUtils.join(strings, "");
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

        //        //cleaning
        //        cleanFiles(fileSystem, asyncQueryResultDir, queryId);
    }
}
