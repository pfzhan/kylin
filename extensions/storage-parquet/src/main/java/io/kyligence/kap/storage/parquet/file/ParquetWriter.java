package io.kyligence.kap.storage.parquet.file;

import io.kyligence.kap.storage.parquet.file.typedwriter.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.HashMap;
import java.util.List;

public class ParquetWriter extends AbstractParquetReaderWriter {
    // TODO: rowsPerPage and pagesPerGroup should be configurable
    private int rowsPerPage = ParquetConfig.RowsPerPage;
    private int pagesPerGroup = ParquetConfig.PagesPerGroup;

    private Configuration conf;
    private ParquetFileWriter writer;
    private ParquetIndexWriter indexWriter;
    private MessageType type;
    private int columnCnt;

    private int currentRowCntInPage = 0;    // Current row number in buffered page
    private int currentPageCntInGroup= 0;   // Current page number in buffered group
    private int currentRowCntInGroup = 0;   // Current row number in buffered rows
    private int currentRowGroup = 0;        // Current total group number

    private Object[][] rowBuffer;           // Buffered rows in current page
    private PageBuffer[][] pageBuffer;      // Buffered pages in current group

    private Encoding rlEncodings;
    private Encoding dlEncodings;
    private List<Encoding> dataEncodings;
    private CompressionCodecName codecName;

    public ParquetWriter(Configuration conf, MessageType type, Path path, Encoding rlEncodings, Encoding dlEncodings, List<Encoding> dataEncodings, CompressionCodecName codecName, Path indexPath) throws IOException {
        writer = new ParquetFileWriter(conf, type, path);
        indexWriter = new ParquetIndexWriter(conf, indexPath);
        this.conf = conf;
        this.type = type;
        this.codecName = codecName;
        this.rlEncodings = rlEncodings;
        this.dlEncodings = dlEncodings;
        this.dataEncodings = dataEncodings;
        columnCnt = type.getColumns().size();

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

    public void open() throws IOException {
        writer.start();
    }

    public void close() throws IOException {
        flush();
        indexWriter.close();
        writer.end(new HashMap<String, String>());
    }

    public void writeRow(byte[] key, int[] keyOffsets, byte[] value, int[] valueLengths) {
    }

    public void writeRow (List<Object> row) throws Exception{
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

    private void flush() throws IOException {
        if (currentRowCntInPage != 0) {
            encodingPage();
        }

        if (currentRowCntInGroup != 0) {
            writeGroup();
        }
    }

    private TypeValuesWriter getValuesWriter(Encoding encoding, ColumnDescriptor descriptor, ValuesType type, int count) {
        ValuesWriter valuesWriter = null;
        switch (encoding) {
            case DELTA_BINARY_PACKED:
                valuesWriter = io.kyligence.kap.storage.parquet.file.Encoding.DELTA_BINARY_PACKED.getValuesWriter(descriptor, type, count);
                break;
            case DELTA_BYTE_ARRAY:
                valuesWriter = io.kyligence.kap.storage.parquet.file.Encoding.DELTA_BYTE_ARRAY.getValuesWriter(descriptor, type, count);
                break;
            case DELTA_LENGTH_BYTE_ARRAY:
                valuesWriter = io.kyligence.kap.storage.parquet.file.Encoding.DELTA_LENGTH_BYTE_ARRAY.getValuesWriter(descriptor, type, count);
                break;
            case PLAIN:
                valuesWriter = io.kyligence.kap.storage.parquet.file.Encoding.PLAIN.getValuesWriter(descriptor, type, count);
                break;
            case RLE:
                valuesWriter = io.kyligence.kap.storage.parquet.file.Encoding.RLE.getValuesWriter(descriptor, type, count);
                break;
            default:
                valuesWriter = null;
                break;
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

    private void encodingPage() {
        for (int i = 0; i < columnCnt; ++i) {
            TypeValuesWriter writer = getValuesWriter(dataEncodings.get(i), type.getColumns().get(i), ValuesType.VALUES, currentRowCntInPage);

            for (int j = 0; j < rowsPerPage; ++j) {
                writer.writeData(rowBuffer[i][j]);
            }
            pageBuffer[i][currentPageCntInGroup] = new PageBuffer(writer.getBytes(), currentRowCntInPage);
        }
        currentPageCntInGroup++;
        currentRowCntInPage = 0;
    }

    private void  writeGroup() throws IOException {
        writer.startBlock(currentRowCntInGroup);
        for (int i = 0; i < columnCnt; ++i) {
            writer.startColumn(type.getColumns().get(i), currentRowCntInGroup, codecName);
            for (int j = 0; j < currentPageCntInGroup; ++j) {
                indexWriter.addIndex(currentRowGroup, i, j, writer.getPos());
                BytesInput bi = pageBuffer[i][j].getBi();
                CompressionCodec compressionCodec = getCodec(codecName, conf);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (compressionCodec == null) {
                    bi.writeAllTo(baos);
                }
                else {
                    CompressionOutputStream os = compressionCodec.createOutputStream(baos,
                            compressionCodec.createCompressor());
                    bi.writeAllTo(os);
                    os.finish();
                    os.close();
                }

                writer.writeDataPage(pageBuffer[i][j].getCount(), (int)bi.size(),
                        BytesInput.from(baos.toByteArray()),
                        Statistics.getStatsBasedOnType(type.getColumns().get(i).getType()),
                        rlEncodings, dlEncodings, dataEncodings.get(i));
            }
            writer.endColumn();
        }
        writer.endBlock();

        currentRowGroup++;
        currentPageCntInGroup = 0;
        currentRowCntInGroup = 0;
    }

    private class PageBuffer{
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