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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.typedwriter.BooleanValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.BytesValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.DoubleValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.IntegerValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.LongValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.TypeValuesWriter;

public class ParquetRawWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRawWriter.class);

    private static final String INDEX_PREFIX = "IndexV2-";

    private int rowsPerPage = ParquetConfig.RowsPerPage;
    private int pagesPerGroup = ParquetConfig.PagesPerGroup;

    private Configuration conf;
    private ParquetFileWriter writer;
    private MessageType schema;
    private int columnCnt;

    private int currentRowCntInPage = 0; // Current row number in buffered page
    private int currentPageCntInGroup = 0; // Current page number in buffered group
    private int currentRowCntInGroup = 0; // Current row number in buffered rows
    private int currentRowGroup = 0; // Current total group number
    private int totalPageCnt = 0; // Total page count by now (consider data written by callee)
    private int totalPageCntToFile = 0; // Total page count by now (consider data written to file), may < totalPageCnt due to local cache

    private Object[][] rowBuffer; // Buffered rows in current page
    private PageBuffer[][] pageBuffer; // Buffered pages in current group

    private Encoding rlEncodings;
    private Encoding dlEncodings;
    private List<Encoding> dataEncodings;
    private CompressionCodecName codecName;
    private ParquetProperties parquetProperties;

    private Map<String, String> indexMap;
    private boolean onIndexV2;

    public ParquetRawWriter(Configuration conf, // hadoop configuration
            MessageType schema, // parquet file row schema
            Path path, // parquet file path
            Encoding rlEncodings, // repeat level encoding
            Encoding dlEncodings, // depth level encoding
            List<Encoding> dataEncodings, // data encoding
            CompressionCodecName codecName, // compression algorithm
            int rowsPerPage, // the number of rows in one page
            int pagesPerGroup, // the number of pages in one row group
            boolean onIndexV2 // if turn on index version 2
    ) throws IOException {
        writer = new ParquetFileWriter(conf, schema, path);
        this.conf = conf;
        this.schema = schema;
        this.codecName = codecName;
        this.rlEncodings = rlEncodings;
        this.dlEncodings = dlEncodings;
        this.dataEncodings = dataEncodings;
        this.rowsPerPage = rowsPerPage;
        this.pagesPerGroup = pagesPerGroup;
        this.onIndexV2 = onIndexV2;
        columnCnt = schema.getColumns().size();
        indexMap = new HashMap<>();
        indexMap.put("pagesPerGroup", String.valueOf(pagesPerGroup));

        parquetProperties = ParquetProperties.builder().build();

        writer.start();
        initRowBuffer();
        initPageBuffer();
    }

    private void initRowBuffer() {
        rowBuffer = new Object[columnCnt][];
        for (int i = 0; i < columnCnt; ++i) {
            rowBuffer[i] = new Object[rowsPerPage];
        }
    }

    public int getPageCntSoFar() {
        return totalPageCnt;
    }

    private void initPageBuffer() {
        pageBuffer = new PageBuffer[columnCnt][];
        for (int i = 0; i < columnCnt; ++i) {
            pageBuffer[i] = new PageBuffer[pagesPerGroup];
        }
    }

    public void close(Map<String, String> addition) throws IOException {
        flush();
        if (addition != null) {
            indexMap.putAll(addition);
        }
        writer.end(indexMap);
    }

    public void close() throws IOException {
        close(null);
    }

    // TODO: this writeRow is not pure, should be refactored
    public void writeRow(byte[] key, int keyOffset, int keyLength, byte[] value, int[] valueLengths) throws Exception {
        List<Object> row = new ArrayList<Object>();
        row.add(Binary.fromConstantByteArray(key, keyOffset, keyLength));

        int valueOffset = 0;
        for (int i = 0; i < valueLengths.length; ++i) {
            row.add(Binary.fromConstantByteArray(value, valueOffset, valueLengths[i]));
            valueOffset += valueLengths[i];
        }

        writeRow(row);
    }

    public void writeRow(List<Object> row) throws Exception {
        // Insert row into buffer
        for (int i = 0; i < row.size(); ++i) {
            rowBuffer[i][currentRowCntInPage] = row.get(i);
        }

        currentRowCntInPage++;
        currentRowCntInGroup++;

        if (currentRowCntInPage == rowsPerPage) {
            encodingPage();
        }

        if (currentPageCntInGroup == pagesPerGroup) {
            writeGroup();
        }
    }

    /**
     * Flush in-mem rows into page buffers.
     * Write in-mem pages into groups.
     * @throws IOException
     */
    public void flush() throws IOException {
        if (currentRowCntInPage != 0) {
            encodingPage();
        }

        if (currentPageCntInGroup != 0) {
            writeGroup();
        }
    }

    private TypeValuesWriter getValuesWriter(ColumnDescriptor descriptor) {
        switch (descriptor.getType()) {
        case BOOLEAN:
            return new BooleanValueWriter(new RunLengthBitPackingHybridValuesWriter(1, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        case INT32:
            return new IntegerValueWriter(new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        case INT64:
            return new LongValueWriter(new DeltaBinaryPackingValuesWriterForLong(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        case INT96:
            return new BytesValueWriter(new FixedLenByteArrayPlainValuesWriter(12, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        case FLOAT:
        case DOUBLE:
            return new DoubleValueWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
            return new BytesValueWriter(new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()));
        default:
            throw new IllegalArgumentException("Unknown type " + descriptor.getType());
        }
    }

    /**
     * Only store encoding pages in buffer, write file only happens in writeGroup.
     */
    private void encodingPage() {
        for (int i = 0; i < columnCnt; ++i) {
            TypeValuesWriter writer = getValuesWriter(schema.getColumns().get(i));

            for (int j = 0; j < currentRowCntInPage; ++j) {
                writer.writeData(rowBuffer[i][j]);
            }
            pageBuffer[i][currentPageCntInGroup] = new PageBuffer(writer.getBytes(), currentRowCntInPage);
        }
        currentPageCntInGroup++;
        currentRowCntInPage = 0;
        totalPageCnt++;
    }

    /**
     * Write both parquet file and index file
     * @throws IOException
     */
    private void writeGroup() throws IOException {
        writer.startBlock(currentRowCntInGroup);
        for (int i = 0; i < columnCnt; ++i) {
            writer.startColumn(schema.getColumns().get(i), currentRowCntInGroup, codecName);
            for (int j = 0; j < currentPageCntInGroup; ++j) {
                if (onIndexV2) {
                    addGlobalPageIndex(currentRowGroup, i, totalPageCntToFile + j, writer.getPos());
                } else {
                    addIndex(currentRowGroup, i, j, writer.getPos());
                }
                BytesInput bi = pageBuffer[i][j].getBi();
                CompressionCodec compressionCodec = CodecFactory.getCodec(codecName, conf);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (compressionCodec == null) {
                    bi.writeAllTo(baos);
                    baos.close();
                } else {
                    CompressionOutputStream os = compressionCodec.createOutputStream(baos, compressionCodec.createCompressor());
                    bi.writeAllTo(os);
                    os.finish();
                    os.close();
                }

                writer.writeDataPage(pageBuffer[i][j].getCount(), (int) bi.size(), BytesInput.from(baos.toByteArray()), Statistics.getStatsBasedOnType(schema.getColumns().get(i).getType()), rlEncodings, dlEncodings, dataEncodings.get(i));
            }
            writer.endColumn();
        }
        writer.endBlock();

        totalPageCntToFile += currentPageCntInGroup;
        currentRowGroup++;
        currentPageCntInGroup = 0;
        currentRowCntInGroup = 0;
    }

    /**
     * Version 1 index
     * Add index to map, make key as "group,column,page"
     * @param group group index
     * @param column column index
     * @param page page index in group
     * @param pos file offset
     */
    private void addIndex(int group, int column, int page, long pos) {
        indexMap.put(group + "," + column + "," + page, String.valueOf(pos));
    }

    /**
     * Version 2 index
     * Add index to map, make key as "IndexPrefix-page,column"
     * @param page global page index
     * @param column column index
     * @param pos file offset
     */
    private void addGlobalPageIndex(int group, int column, int page, long pos) {
        indexMap.put(INDEX_PREFIX + page + "," + column, group + "," + String.valueOf(pos));
    }

    private class PageBuffer {
        private BytesInput bi;
        private int count;

        PageBuffer(BytesInput bi, int count) {
            this.bi = bi;
            this.count = count;
        }

        public BytesInput getBi() {
            return bi;
        }

        public int getCount() {
            return count;
        }
    }

    public static class Builder {
        protected static final Logger logger = LoggerFactory.getLogger(Builder.class);

        private Configuration conf = null;
        private MessageType type = null;
        private Path path = null;
        private Encoding rlEncodings = Encoding.RLE;
        private Encoding dlEncodings = Encoding.RLE;
        private List<Encoding> dataEncodings = null;
        private CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
        private int rowsPerPage = ParquetConfig.RowsPerPage;
        private int pagesPerGroup = ParquetConfig.PagesPerGroup;
        private boolean onIndexV2 = true;

        public Builder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder setType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }

        public Builder setRlEncodings(Encoding rlEncodings) {
            this.rlEncodings = rlEncodings;
            return this;
        }

        public Builder setDlEncodings(Encoding dlEncodings) {
            this.dlEncodings = dlEncodings;
            return this;
        }

        public Builder setDataEncodings(List<Encoding> dataEncodings) {
            this.dataEncodings = dataEncodings;
            return this;
        }

        public Builder setOnIndexV2(boolean on) {
            this.onIndexV2 = on;
            return this;
        }

        public Builder setCodecName(String codecName) {
            if (StringUtils.isEmpty(codecName)) {
                this.codecName = CompressionCodecName.UNCOMPRESSED;
            }

            CompressionCodecName compressionCodecName;
            try {
                compressionCodecName = CompressionCodecName.valueOf(codecName.toUpperCase());
            } catch (Exception e) {
                compressionCodecName = CompressionCodecName.UNCOMPRESSED;
            }

            this.codecName = compressionCodecName;
            logger.info("The chosen CompressionCodecName is " + this.codecName);
            return this;
        }

        public Builder setRowsPerPage(int rowsPerPage) {
            this.rowsPerPage = rowsPerPage;
            return this;
        }

        public Builder setPagesPerGroup(int pagesPerGroup) {
            this.pagesPerGroup = pagesPerGroup;
            return this;
        }

        public Builder() {
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

            logger.info("Builder: rowsPerPage={}", rowsPerPage);
            logger.info("write file: {}", path.toString());
            return new ParquetRawWriter(conf, type, path, rlEncodings, dlEncodings, dataEncodings, codecName, rowsPerPage, pagesPerGroup, onIndexV2);
        }
    }
}