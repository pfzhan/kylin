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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRawReader {
    public static final Logger logger = LoggerFactory.getLogger(ParquetRawReader.class);

    private static final String INDEX_PREFIX = "IndexV2-";
    private ParquetMetadata parquetMetadata;
    private FSDataInputStream inputStream;
    private Configuration config;
    private long fileOffset;
    private static ThreadLocal<Map<CompressionCodecName, Decompressor>> decompressorMap = new ThreadLocal<>();

    protected int pagesPerGroup = 0;
    protected Map<String, String> indexMap;

    public ParquetRawReader(Configuration configuration, Path path, ParquetMetadata metadata, long fileOffset) throws IOException {
        this.config = configuration;
        if (metadata == null) {
            this.parquetMetadata = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
        } else {
            this.parquetMetadata = metadata;
        }

        if (decompressorMap.get() == null) {
            decompressorMap.set(new HashMap<CompressionCodecName, Decompressor>());
        }

        FileSystem fileSystem = path.getFileSystem(configuration);
        this.inputStream = fileSystem.open(path);

        this.indexMap = parquetMetadata.getFileMetaData().getKeyValueMetaData();
        this.pagesPerGroup = Integer.parseInt(indexMap.get("pagesPerGroup"));
        this.fileOffset = fileOffset;
        logger.info("The file offset is " + this.fileOffset);
    }

    // This function is used for test, package visible
    ParquetMetadata getParquetMetadata() {
        return parquetMetadata;
    }

    public MessageType getSchema() {
        return parquetMetadata.getFileMetaData().getSchema();
    }

    public void close() throws IOException {
        inputStream.close();
//        indexReader.close();
    }

    /**
     * Get page values reader according to global page index
     * @param globalPageIndex global page index starting from the first page
     * @param column the column to be read
     * @return values reader, if returns null, there's no such page
     */
    public GeneralValuesReader getValuesReader(int globalPageIndex, int column) throws IOException {
        long offset = 0L;
        int group = 0;

        if(indexMap.containsKey(INDEX_PREFIX + globalPageIndex + "," + column)) {
            // index version 2
            String[] index = indexMap.get(INDEX_PREFIX + globalPageIndex + "," + column).split(",");
            group = Integer.valueOf(index[0]);
            offset = Long.valueOf(index[1]);
        } else {
            // index version 1
            group = globalPageIndex / pagesPerGroup;
            int page = globalPageIndex % pagesPerGroup;
            if (!indexMap.containsKey(group + "," + column + "," + page)) {
                return null;
            }
            offset = Long.parseLong(indexMap.get(group + "," + column + "," + page));
        }
        return getValuesReaderFromOffset(group, column, offset + fileOffset);
    }

    public GeneralValuesReader getValuesReader(int rowGroup, int column, int pageIndex) throws IOException {
        long pageOffset = Long.parseLong(indexMap.get(rowGroup + "," + column + "," + pageIndex));
        return getValuesReaderFromOffset(rowGroup, column, pageOffset + fileOffset);
    }

    public int getColumnCount() {
        return parquetMetadata.getFileMetaData().getSchema().getColumns().size();
    }

    private Decompressor getDecompressorByName(CompressionCodecName codecName) {
        Map<CompressionCodecName, Decompressor> localDecompressorMap = decompressorMap.get();
        Decompressor decompressor = null;
        if (localDecompressorMap.containsKey(codecName)) {
            decompressor = localDecompressorMap.get(codecName);
        } else {
            CompressionCodec codec = CodecFactory.getCodec(codecName, config);
            if (codec != null) {
                decompressor = CodecFactory.getCodec(codecName, config).createDecompressor();
            }
            localDecompressorMap.put(codecName, decompressor);
        }
        return decompressor;
    }

    protected GeneralValuesReader getValuesReaderFromOffset(int rowGroup, int column, long offset) throws IOException {
        BlockMetaData blockMetaData = parquetMetadata.getBlocks().get(rowGroup);
        ColumnChunkMetaData columnChunkMetaData = blockMetaData.getColumns().get(column);

        ColumnDescriptor columnDescriptor = getSchema().getColumns().get(column);
        CompressionCodecName codecName = columnChunkMetaData.getCodec();
        Decompressor decompressor = getDecompressorByName(codecName);

        inputStream.seek(offset);
        PageHeader pageHeader = Util.readPageHeader(inputStream);
        if (pageHeader.getType() == PageType.DATA_PAGE) {
            DataPageHeader dataPageHeader = pageHeader.getData_page_header();
            int numValues = dataPageHeader.getNum_values();
            byte[] decompressedDataBytes = readAndDecompress(codecName, decompressor,
                    pageHeader.getCompressed_page_size(), pageHeader.getUncompressed_page_size());

            offset = skipLevels(numValues, columnDescriptor, dataPageHeader.getRepetition_level_encoding(), dataPageHeader.getDefinition_level_encoding(), decompressedDataBytes, 0);

            ValuesReader dataReader = getValuesReader(dataPageHeader.getEncoding(), columnDescriptor, ValuesType.VALUES);
            dataReader.initFromPage(numValues, ByteBuffer.wrap(decompressedDataBytes), (int) offset);
            return new GeneralValuesReader.Builder().setLength(numValues).setReader(dataReader).setType(columnChunkMetaData.getType()).build();
        } else if (pageHeader.getType() == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 dataPageHeader = pageHeader.getData_page_header_v2();
            int numValues = dataPageHeader.getNum_values();

            // Skip levels
            inputStream.seek(inputStream.getPos() + dataPageHeader.repetition_levels_byte_length + dataPageHeader.definition_levels_byte_length);

            byte[] decompressedDataBytes;
            if (dataPageHeader.is_compressed) {
                decompressedDataBytes = readAndDecompress(codecName, decompressor, pageHeader.getCompressed_page_size(),
                        pageHeader.getUncompressed_page_size());
            } else {
                assert (pageHeader.getCompressed_page_size() == pageHeader.getUncompressed_page_size());
                decompressedDataBytes = readAsBytesInput(pageHeader.getCompressed_page_size());
            }
            ValuesReader dataReader = getValuesReader(dataPageHeader.getEncoding(), columnDescriptor, ValuesType.VALUES);
            dataReader.initFromPage(numValues, ByteBuffer.wrap(decompressedDataBytes), 0);
            return new GeneralValuesReader.Builder().setLength(numValues).setReader(dataReader).setType(columnChunkMetaData.getType()).build();
        }
        return null;
    }

    private int skipLevels(int numValues, ColumnDescriptor descriptor, Encoding rEncoding, Encoding dEncoding, byte[] in, int offset) throws IOException {
        offset = skipRepetitionLevel(numValues, descriptor, rEncoding, in, offset).getNextOffset();
        offset = skipDefinitionLevel(numValues, descriptor, dEncoding, in, offset).getNextOffset();
        return offset;
    }

    private ValuesReader skipRepetitionLevel(int numValues, ColumnDescriptor descriptor, Encoding encoding, byte[] in, int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.REPETITION_LEVEL, in, offset);
    }

    private ValuesReader skipDefinitionLevel(int numValues, ColumnDescriptor descriptor, Encoding encoding, byte[] in, int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.DEFINITION_LEVEL, in, offset);
    }

    private ValuesReader skipLevel(int numValues, ColumnDescriptor descriptor, Encoding encoding, ValuesType type, byte[] in, int offset) throws IOException {
        ValuesReader reader = getValuesReader(encoding, descriptor, type);
        reader.initFromPage(numValues, ByteBuffer.wrap(in), offset);
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
    private byte[] readAndDecompress(CompressionCodecName codec, Decompressor decompressor, int compressedSize,
            int uncompressedSize) throws IOException {
        if (decompressor == null) {
            byte[] buffer = new byte[compressedSize];
            inputStream.readFully(buffer, 0, compressedSize);
            return buffer;
        } else {
            CompressionCodec compressionCodec = CodecFactory.getCodec(codec, config);
            byte[] buffer = new byte[uncompressedSize];
            InputStream is = compressionCodec.createInputStream(new BoundedInputStream(inputStream, compressedSize),
                    decompressor);
            is.read(buffer, 0, uncompressedSize);
            return buffer;
        }
    }

    private byte[] readAsBytesInput(int size) throws IOException {
        byte[] buffer = new byte[size];
        inputStream.readFully(buffer, 0, size);
        return buffer;
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

    public static class Builder {
        private Configuration conf = null;
        private ParquetMetadata metadata = null;
        private Path path = null;
        private int fileOffset = 0;//if it's a tarball fileoffset is not 0

        public Builder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder setMetadata(ParquetMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }

        public Builder setFileOffset(int fileOffset) {
            this.fileOffset = fileOffset;
            return this;
        }

        public ParquetRawReader build() throws IOException {
            if (conf == null) {
                throw new IllegalStateException("Configuration should be set");
            }

            if (path == null) {
                throw new IllegalStateException("Output file path should be set");
            }

            if (fileOffset < 0) {
                throw new IllegalStateException("File offset is " + fileOffset);
            }

            return new ParquetRawReader(conf, path, metadata, fileOffset);
        }
    }
}
