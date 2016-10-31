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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.typedwriter.BooleanValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.BytesValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.DoubleValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.FloatValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.IntegerValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.LongValueWriter;
import io.kyligence.kap.storage.parquet.format.file.typedwriter.TypeValuesWriter;

public class ParquetRawWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRawWriter.class);

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

    private Object[][] rowBuffer; // Buffered rows in current page
    private PageBuffer[][] pageBuffer; // Buffered pages in current group

    private Encoding rlEncodings;
    private Encoding dlEncodings;
    private List<Encoding> dataEncodings;
    private CompressionCodecName codecName;

    private Map<String, String> indexMap;

    public ParquetRawWriter(Configuration conf, // hadoop configuration
            MessageType schema, // parquet file row schema
            Path path, // parquet file path
            Encoding rlEncodings, // repeat level encoding
            Encoding dlEncodings, // depth level encoding
            List<Encoding> dataEncodings, // data encoding
            CompressionCodecName codecName, // compression algorithm
            Path indexPath, // parquet index file path
            int rowsPerPage, // the number of rows in one page
            int pagesPerGroup // the number of pages in one row group
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
        columnCnt = schema.getColumns().size();
        indexMap = new HashMap<>();
        indexMap.put("pagesPerGroup", String.valueOf(pagesPerGroup));

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

    private void initPageBuffer() {
        pageBuffer = new PageBuffer[columnCnt][];
        for (int i = 0; i < columnCnt; ++i) {
            pageBuffer[i] = new PageBuffer[pagesPerGroup];
        }
    }

    public void close() throws IOException {
        // close parquet file
        flush();
        writer.end(indexMap);
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
    private void flush() throws IOException {
        if (currentRowCntInPage != 0) {
            encodingPage();
        }

        if (currentPageCntInGroup != 0) {
            writeGroup();
        }
    }

    private TypeValuesWriter getValuesWriter(Encoding encoding, ColumnDescriptor descriptor, ValuesType type, int count) {
        ValuesWriter valuesWriter = null;
        switch (encoding) {
        case DELTA_BINARY_PACKED:
            valuesWriter = io.kyligence.kap.storage.parquet.format.file.Encoding.DELTA_BINARY_PACKED.getValuesWriter(descriptor, type, count);
            break;
        case DELTA_BYTE_ARRAY:
            valuesWriter = io.kyligence.kap.storage.parquet.format.file.Encoding.DELTA_BYTE_ARRAY.getValuesWriter(descriptor, type, count);
            break;
        case DELTA_LENGTH_BYTE_ARRAY:
            valuesWriter = io.kyligence.kap.storage.parquet.format.file.Encoding.DELTA_LENGTH_BYTE_ARRAY.getValuesWriter(descriptor, type, count);
            break;
        case PLAIN:
            valuesWriter = io.kyligence.kap.storage.parquet.format.file.Encoding.PLAIN.getValuesWriter(descriptor, type, count);
            break;
        case RLE:
            valuesWriter = io.kyligence.kap.storage.parquet.format.file.Encoding.RLE.getValuesWriter(descriptor, type, count);
            break;
        default:
            valuesWriter = null;
            break;
        }

        if (valuesWriter == null) {
            return null;
        }

        switch (descriptor.getType()) {
        case BOOLEAN:
            return new BooleanValueWriter(valuesWriter);
        case INT32:
            return new IntegerValueWriter(valuesWriter);
        case INT64:
            return new LongValueWriter(valuesWriter);
        case INT96:
            return new BytesValueWriter(valuesWriter);
        case FLOAT:
            return new FloatValueWriter(valuesWriter);
        case DOUBLE:
            return new DoubleValueWriter(valuesWriter);
        case FIXED_LEN_BYTE_ARRAY:
            return new BytesValueWriter(valuesWriter);
        case BINARY:
            return new BytesValueWriter(valuesWriter);
        default:
            return null;
        }
    }

    /**
     * Only store encoding pages in buffer, write file only happens in writeGroup.
     */
    private void encodingPage() {
        for (int i = 0; i < columnCnt; ++i) {
            TypeValuesWriter writer = getValuesWriter(dataEncodings.get(i), schema.getColumns().get(i), ValuesType.VALUES, currentRowCntInPage);

            for (int j = 0; j < currentRowCntInPage; ++j) {
                writer.writeData(rowBuffer[i][j]);
            }
            pageBuffer[i][currentPageCntInGroup] = new PageBuffer(writer.getBytes(), currentRowCntInPage);
        }
        currentPageCntInGroup++;
        currentRowCntInPage = 0;
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
                addIndex(currentRowGroup, i, j, writer.getPos());
                BytesInput bi = pageBuffer[i][j].getBi();
                CompressionCodec compressionCodec = CodecFactory.getCodec(codecName, conf);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (compressionCodec == null) {
                    bi.writeAllTo(baos);
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

        currentRowGroup++;
        currentPageCntInGroup = 0;
        currentRowCntInGroup = 0;
    }

    private void addIndex(int group, int column, int page, long pos) {
        indexMap.put(group + "," + column + "," + page, String.valueOf(pos));
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
}