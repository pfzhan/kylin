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

import com.clearspring.analytics.util.Lists;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CSVExcelWriter {

    private static final Logger logger = LoggerFactory.getLogger("query");

    public void writeData(FileStatus[] fileStatuses, OutputStream outputStream) throws IOException {
        CsvListWriter csvWriter = null;
        Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        Boolean firstWrite = true;
        csvWriter = new CsvListWriter(writer, CsvPreference.STANDARD_PREFERENCE);
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.getPath().getName().startsWith("_")) {
                if (fileStatus.getPath().getName().endsWith("parquet")) {
                    if (Boolean.TRUE.equals(firstWrite)) {
                        writer.write('\uFEFF');
                        firstWrite = false;
                    }
                    writeDataByParquet(fileStatus, csvWriter);
                } else {
                    writeDataByCsv(fileStatus, outputStream);
                }
            }
        }
        csvWriter.close();
    }

    private void writeDataByParquet(FileStatus fileStatus, CsvListWriter csvWriter) {
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read().parquet(fileStatus.getPath().toString()).collectAsList();
        rowList.stream().forEach(row -> {
            List<String> result = Lists.newArrayList();
            val list = row.toSeq().toList();
            for (int i = 0; i < list.size(); i++) {
                Object cell = list.apply(i);
                String column = cell == null ? "" : cell.toString();
                result.add(column);
            }
            try {
                csvWriter.write(result);
            } catch (IOException e) {
                logger.error("Failed to download asyncQueryResult csvExcel by parquet", e);
            }
        });
    }

    private void writeDataByCsv(FileStatus fileStatus, OutputStream outputStream) {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        try (FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath())) {
            IOUtils.copy(inputStream, outputStream);
        } catch (Exception e) {
            logger.error("Failed to download asyncQueryResult csvExcel by csv", e);
        }
    }

}
