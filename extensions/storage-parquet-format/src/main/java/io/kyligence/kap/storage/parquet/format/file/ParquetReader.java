package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.format.*;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.List;

public class ParquetReader extends AbstractParquetReaderWriter {
    private ParquetMetadata parquetMetadata;
    private FSDataInputStream inputStream;
    private ParquetIndexReader indexReader;
    private Configuration config;

    private int column;
    private int curPage;

    public ParquetReader(Configuration configuration, Path path, Path indexPath, int column) throws IOException{
        config = configuration;
        parquetMetadata = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
        FileSystem fileSystem = FileSystem.get(config);
        inputStream = fileSystem.open(path);

        indexReader = new ParquetIndexReader(configuration, indexPath);
        this.column = column;
        this.curPage = 0;
    }

    public MessageType getSchema() {
        return parquetMetadata.getFileMetaData().getSchema();
    }

    public void close() throws IOException {
        inputStream.close();
        indexReader.close();
    }

    /**
     * Get next page values reader
     * @return values reader, if returns null, there's no page left
     */
    public GeneralValuesReader getNextValuesReader() throws IOException {
        return getValuesReader(curPage++, column);
    }

    /**
     * Get page values reader according to global page index
     * @param globalPageIndex global page index starting from the first page
     * @return values reader, if returns null, there's no such page
     */
    public GeneralValuesReader getValuesReader(int globalPageIndex, int column) throws IOException {
        List<ParquetIndexReader.IndexBundle> indexBundleList = indexReader.getOffsets(column);
        if (globalPageIndex >= indexBundleList.size()) {
            return null;
        }
        ParquetIndexReader.IndexBundle indexBundle = indexBundleList.get(globalPageIndex);
        return getValuesReaderFromOffset(indexBundle.getGroup(), column, indexBundle.getIndex());
    }

    public GeneralValuesReader getValuesReader(int rowGroup, int column, int pageIndex) throws IOException {
        long pageOffset = indexReader.getOffset(rowGroup, column, pageIndex);
        return getValuesReaderFromOffset(rowGroup, column, pageOffset);
    }

    public int getColumnCount() {
        return parquetMetadata.getFileMetaData().getSchema().getColumns().size();
    }

    private GeneralValuesReader getValuesReaderFromOffset(int rowGroup, int column, long offset) throws IOException {
        BlockMetaData blockMetaData = parquetMetadata.getBlocks().get(rowGroup);
        ColumnChunkMetaData columnChunkMetaData = blockMetaData.getColumns().get(column);

        ColumnDescriptor columnDescriptor = getSchema().getColumns().get(column);
        CompressionCodecName codecName = columnChunkMetaData.getCodec();
        CompressionCodec codec = getCodec(codecName, config);
        Decompressor decompressor = null;
        if (codec != null) {
            decompressor = getCodec(codecName,config).createDecompressor();
        }

        inputStream.seek(offset);
        PageHeader pageHeader= Util.readPageHeader(inputStream);
        if (pageHeader.getType() == PageType.DATA_PAGE) {
            DataPageHeader dataPageHeader = pageHeader.getData_page_header();
            int numValues = dataPageHeader.getNum_values();
            BytesInput decompressedData = readAndDecompress(codecName,
                    decompressor,
                    pageHeader.getCompressed_page_size(),
                    pageHeader.getUncompressed_page_size());
            byte[] decompressedDataBytes = decompressedData.toByteArray();

            offset = skipLevels(numValues,
                    columnDescriptor,
                    dataPageHeader.getRepetition_level_encoding(),
                    dataPageHeader.getDefinition_level_encoding(),
                    decompressedDataBytes,
                    0);

            ValuesReader dataReader = getValuesReader(dataPageHeader.getEncoding(), columnDescriptor, ValuesType.VALUES);
            dataReader.initFromPage(numValues, decompressedDataBytes, (int)offset);
            return new GeneralValuesReaderBuilder().setLength(numValues).setReader(dataReader).setType(columnChunkMetaData.getType()).build();
        } else if (pageHeader.getType() == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 dataPageHeader = pageHeader.getData_page_header_v2();
            int numValues = dataPageHeader.getNum_values();

            // Skip levels
            inputStream.seek(inputStream.getPos() +
                    dataPageHeader.repetition_levels_byte_length +
                    dataPageHeader.definition_levels_byte_length);

            BytesInput decompressedData;
            if (dataPageHeader.is_compressed) {
                decompressedData = readAndDecompress(codecName,
                        decompressor,
                        pageHeader.getCompressed_page_size(),
                        pageHeader.getUncompressed_page_size());
            } else {
                assert(pageHeader.getCompressed_page_size() == pageHeader.getUncompressed_page_size());
                decompressedData = readAsBytesInput(pageHeader.getCompressed_page_size());
            }
            byte[] decompressedDataBytes = decompressedData.toByteArray();
            ValuesReader dataReader = getValuesReader(dataPageHeader.getEncoding(), columnDescriptor, ValuesType.VALUES);
            dataReader.initFromPage(numValues, decompressedDataBytes, 0);
            return new GeneralValuesReaderBuilder().setLength(numValues).setReader(dataReader).setType(columnChunkMetaData.getType()).build();
        }
        return null;
    }

    private int skipLevels(int numValues,
                           ColumnDescriptor descriptor,
                           Encoding rEncoding,
                           Encoding dEncoding,
                           byte[] in,
                           int offset) throws IOException {
        offset = skipRepetitionLevel(numValues, descriptor, rEncoding, in, offset).getNextOffset();
        offset = skipDefinitionLevel(numValues, descriptor, dEncoding, in, offset).getNextOffset();
        return offset;
    }

    private ValuesReader skipRepetitionLevel(int numValues,
                                             ColumnDescriptor descriptor,
                                             Encoding encoding,
                                             byte[] in, int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.REPETITION_LEVEL, in, offset);
    }
    private ValuesReader skipDefinitionLevel(int numValues,
                                     ColumnDescriptor descriptor,
                                     Encoding encoding,
                                     byte[] in,
                                     int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.DEFINITION_LEVEL, in, offset);
    }

    private ValuesReader skipLevel(int numValues,
                           ColumnDescriptor descriptor,
                           Encoding encoding,
                           ValuesType type,
                           byte[] in,
                           int offset) throws IOException {
        ValuesReader reader = getValuesReader(encoding, descriptor, type);
        reader.initFromPage(numValues, in, offset);
        return reader;
    }

    private ValuesReader getValuesReader(Encoding encoding, ColumnDescriptor descriptor, ValuesType type) {
        switch (encoding) {
            case BIT_PACKED:
                return org.apache.parquet.column.Encoding.BIT_PACKED.getValuesReader(descriptor, type);
            case DELTA_BINARY_PACKED:
                return org.apache.parquet.column.Encoding.DELTA_BINARY_PACKED.getValuesReader(descriptor, type);
            case DELTA_BYTE_ARRAY:
                return org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY.getValuesReader(descriptor, type);
            case DELTA_LENGTH_BYTE_ARRAY:
                return org.apache.parquet.column.Encoding.DELTA_LENGTH_BYTE_ARRAY.getValuesReader(descriptor, type);
            case PLAIN:
                return org.apache.parquet.column.Encoding.PLAIN.getValuesReader(descriptor, type);
            case PLAIN_DICTIONARY:
                return org.apache.parquet.column.Encoding.PLAIN_DICTIONARY.getValuesReader(descriptor, type);
            case RLE:
                return org.apache.parquet.column.Encoding.RLE.getValuesReader(descriptor, type);
            case RLE_DICTIONARY:
                return org.apache.parquet.column.Encoding.RLE_DICTIONARY.getValuesReader(descriptor, type);
            default:
                return null;
        }
    }

    // TODO: refactor these wrapper to improve performance
    private BytesInput readAndDecompress(CompressionCodecName codec,
                                         Decompressor decompressor,
                                         int compressedSize,
                                         int uncompressedSize) throws IOException{
        CompressionCodec compressionCodec = getCodec(codec, config);
        BytesInput compressedData = readAsBytesInput(compressedSize);
        if (decompressor == null) {
            return compressedData;
        }
        else {
            InputStream is = compressionCodec.createInputStream(new ByteArrayInputStream(compressedData.toByteArray()),
                    decompressor);
            return BytesInput.from(is, uncompressedSize);
        }
    }

    private BytesInput readAsBytesInput(int size) throws IOException {
        final BytesInput r = BytesInput.from(inputStream, size);
        return r;
    }


    private long jumpToPage(int index, long pageOffset) throws IOException {
        inputStream.seek(pageOffset);

        for (int i = 0; i < index; ++i) {
            pageOffset = skipPage();
        }

        return pageOffset;
    }

    private long skipPage() throws IOException {
        PageHeader header = Util.readPageHeader(inputStream);
        long pageOffset = inputStream.getPos() + header.getCompressed_page_size();
        inputStream.seek(pageOffset);

        return pageOffset;
    }
}
