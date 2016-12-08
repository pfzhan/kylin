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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRawWriterBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetRawWriterBuilder.class);

    private Configuration conf = null;
    private MessageType type = null;
    private Path path = null;
    private Encoding rlEncodings = Encoding.RLE;
    private Encoding dlEncodings = Encoding.RLE;
    private List<Encoding> dataEncodings = null;
    private CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    private int rowsPerPage = 10000;
    private int pagesPerGroup = 100;

    public ParquetRawWriterBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetRawWriterBuilder setType(MessageType type) {
        this.type = type;
        return this;
    }

    public ParquetRawWriterBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetRawWriterBuilder setRlEncodings(Encoding rlEncodings) {
        this.rlEncodings = rlEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setDlEncodings(Encoding dlEncodings) {
        this.dlEncodings = dlEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setDataEncodings(List<Encoding> dataEncodings) {
        this.dataEncodings = dataEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setCodecName(String codecName) {
        if (StringUtils.isEmpty(codecName)) {
            this.codecName = CompressionCodecName.UNCOMPRESSED;
        }

        CompressionCodecName compressionCodecName;
        try {
            compressionCodecName = CompressionCodecName.valueOf(codecName.toUpperCase());
        } catch (Exception e) {
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        }

        logger.info("The chosen CompressionCodecName is " + this.codecName);
        this.codecName = compressionCodecName;
        return this;
    }

    public ParquetRawWriterBuilder setRowsPerPage(int rowsPerPage) {
        this.rowsPerPage = rowsPerPage;
        return this;
    }

    public ParquetRawWriterBuilder setPagesPerGroup(int pagesPerGroup) {
        this.pagesPerGroup = pagesPerGroup;
        return this;
    }

    public ParquetRawWriterBuilder() {
    }

    public ParquetRawWriter build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }
        if (type == null) {
            throw new IllegalStateException("Schema should be set");
        }
        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        if (dataEncodings == null) {
            dataEncodings = new ArrayList<Encoding>();
            for (int i = 0; i < type.getColumns().size(); ++i) {
                switch (type.getColumns().get(i).getType()) {
                case BOOLEAN:
                    dataEncodings.add(Encoding.RLE);
                    break;
                case INT32:
                case INT64:
                    dataEncodings.add(Encoding.DELTA_BINARY_PACKED);
                    break;
                case INT96:
                case FLOAT:
                case DOUBLE:
                    dataEncodings.add(Encoding.PLAIN);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                    dataEncodings.add(Encoding.DELTA_BYTE_ARRAY);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type.getColumns().get(i).getType());
                }
            }
        }

        logger.info("ParquetRawWriterBuilder: rowsPerPage={}", rowsPerPage);
        logger.info("write file: {}", path.toString());
        return new ParquetRawWriter(conf, type, path, rlEncodings, dlEncodings, dataEncodings, codecName, rowsPerPage, pagesPerGroup);
    }
}
