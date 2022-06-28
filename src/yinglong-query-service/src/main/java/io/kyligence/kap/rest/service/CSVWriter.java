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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import scala.collection.JavaConverters;

public class CSVWriter {

    private static final Logger logger = LoggerFactory.getLogger("query");

    private static final String QUOTE_CHAR = "\"";
    private static final String END_OF_LINE_SYMBOLS = IOUtils.LINE_SEPARATOR_UNIX;

    public void writeData(FileStatus[] fileStatuses, OutputStream outputStream,
                          String columnNames, String separator, boolean includeHeaders) throws IOException {

        try (Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
            if (includeHeaders) {
                writer.write(columnNames.replace(",", separator));
                writer.flush();
            }
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().startsWith("_")) {
                    if (fileStatus.getPath().getName().endsWith("parquet")) {
                        writeDataByParquet(fileStatus, writer, separator);
                    } else {
                        writeDataByCsv(fileStatus, writer, separator);
                    }
                }
            }

            writer.flush();
        }
    }

    public static void writeCsv(Iterator<List<Object>> rows, Writer writer, String separator) {
        rows.forEachRemaining(row -> {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < row.size(); i++) {
                Object cell = row.get(i);
                String column = cell == null ? "" : cell.toString();

                if (i > 0) {
                    builder.append(separator);
                }

                final String escapedCsv = encodeCell(column, separator);
                builder.append(escapedCsv);
            }
            builder.append(END_OF_LINE_SYMBOLS); // EOL
            try {
                writer.write(builder.toString());
            } catch (IOException e) {
                logger.error("Failed to download asyncQueryResult csvExcel by parquet", e);
            }
        });
    }

    private void writeDataByParquet(FileStatus fileStatus, Writer writer, String separator) {
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .parquet(fileStatus.getPath().toString()).collectAsList();
        writeCsv(rowList.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq())).iterator(), writer, separator);
    }

    // the encode logic is copied from org.supercsv.encoder.DefaultCsvEncoder.encode
    private static String encodeCell(String cell, String separator) {

        boolean needQuote = cell.contains(separator) || cell.contains("\r") || cell.contains("\n");

        if (cell.contains(QUOTE_CHAR)) {
            needQuote = true;
            // escape
            cell = cell.replace(QUOTE_CHAR, QUOTE_CHAR + QUOTE_CHAR);
        }

        if (needQuote) {
            return QUOTE_CHAR + cell + QUOTE_CHAR;
        } else {
            return cell;
        }
    }

    private void writeDataByCsv(FileStatus fileStatus, Writer writer, String separator) {
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .csv(fileStatus.getPath().toString()).collectAsList();
        writeCsv(rowList.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq())).iterator(), writer, separator);
    }

}
