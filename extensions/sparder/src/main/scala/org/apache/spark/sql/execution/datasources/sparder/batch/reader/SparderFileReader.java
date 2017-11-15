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

package org.apache.spark.sql.execution.datasources.sparder.batch.reader;

import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.kylin.common.util.Pair;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.file.ParquetMetrics;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawReader;

public class SparderFileReader extends ParquetRawReader {
    private static final Logger logger = LoggerFactory.getLogger(SparderFileReader.class);

    private Map<Integer, Map<Integer, List<Long>>> rowGroupMapping;
    private Integer currentBlock = 0;
    private List<ColumnDescriptor> columnDescriptors;
    private static ParquetMetadataConverter converter = new ParquetMetadataConverter();
    private Configuration configuration;
    ArrayList<Integer> blockIndex;
    List<BlockMetaData> allBolocks;

    public SparderFileReader(Configuration configuration, Path path, ParquetMetadata metadata, ParquetMetrics metrics,
            long fileOffset, ImmutableRoaringBitmap pageBitset, ImmutableRoaringBitmap columns) throws IOException {
        super(configuration, path, metadata, metrics, fileOffset);
        this.configuration = configuration;
        allBolocks = parquetMetadata.getBlocks();
        columnDescriptors = parquetMetadata.getFileMetaData().getSchema().getColumns();
        this.rowGroupMapping = Maps.newTreeMap();
        initRowGroupMapping(pageBitset, columns);
        blockIndex = Lists.newArrayList(rowGroupMapping.keySet());
        Collections.sort(blockIndex);
    }

    private Pair<Integer, Long> pageIndexToGroupIndex(int pageIndex, int column) {
        String key = INDEX_PREFIX + pageIndex + "," + column;
        Long offset;
        Integer group;
        if (indexMap.containsKey(key)) {
            // index version 2
            String indexStr = indexMap.get(key);
            if (indexStr == null) {
                return null;
            }
            int cut = indexStr.indexOf(',');
            offset = Long.valueOf(indexStr.substring(cut + 1));
            group = Integer.valueOf(indexStr.substring(0, cut));
            return new Pair<>(group, offset);

        } else {
            // index version 1
            group = pageIndex / pagesPerGroup;
            int page = pageIndex % pagesPerGroup;
            if (!indexMap.containsKey(group + "," + "," + page)) {
                return null;
            }
            offset = Long.parseLong(indexMap.get(group + "," + column + "," + page));

            return new Pair<>(group, offset);
        }

    }

    private void initRowGroupMapping(ImmutableRoaringBitmap pageBitset, ImmutableRoaringBitmap columns) {
        for (Integer pageIndex : pageBitset) {
            for (Integer columnIndex : columns) {
                Pair<Integer, Long> pair = pageIndexToGroupIndex(pageIndex, columnIndex);
                if (pair == null)
                    continue;
                if (rowGroupMapping.containsKey(pair.getFirst())) {
                    Map<Integer, List<Long>> pageMapping = rowGroupMapping.get(pair.getFirst());
                    addIndex(columnIndex, pair, pageMapping);

                } else {
                    Map<Integer, List<Long>> pageMapping = Maps.newHashMap();
                    rowGroupMapping.put(pair.getFirst(), pageMapping);
                    addIndex(columnIndex, pair, pageMapping);
                }
            }
        }
    }

    private void addIndex(Integer columnIndex, Pair<Integer, Long> pair, Map<Integer, List<Long>> pageMapping) {
        if (pageMapping.containsKey(columnIndex)) {
            pageMapping.get(columnIndex).add(pair.getSecond() + fileOffset);
        } else {
            List<Long> list = Lists.newArrayList();
            list.add(pair.getSecond() + fileOffset);
            pageMapping.put(columnIndex, list);
        }
    }

    public ColumnChunkPageReadStore readNextRowGroup() throws IOException {
        if (currentBlock == rowGroupMapping.size()) {
            return null;
        }
        ParquetMetrics.get().groupReadStart();
        Integer blockIdx = blockIndex.get(currentBlock);
        BlockMetaData block = allBolocks.get(blockIdx);
        List<ColumnChunkMetaData> columns = block.getColumns();
        Map<Integer, List<Long>> pageMapping = rowGroupMapping.get(blockIdx);
        List<ConsecutiveChunkList> allChunks = new ArrayList<>();
        ColumnChunkPageReadStore columnChunkPageReadStore = new ColumnChunkPageReadStore(block.getRowCount());
        ConsecutiveChunkList currentChunks = null;
        for (Integer columnIndex : pageMapping.keySet()) {
            ColumnChunkMetaData mc = columns.get(columnIndex);
            long startingPos = mc.getStartingPos() + fileOffset;
            if (currentChunks == null || currentChunks.endPos() != startingPos) {
                currentChunks = new ConsecutiveChunkList(startingPos);
                allChunks.add(currentChunks);
            }
            currentChunks.addChunk(new ChunkDescriptor(columnDescriptors.get(columnIndex), mc, startingPos,
                    (int) mc.getTotalSize(), pageMapping.get(columnIndex), columnIndex));
        }
        for (ConsecutiveChunkList consecutiveChunks : allChunks) {
            final List<Chunk> chunks = consecutiveChunks.readAll(inputStream);
            for (Chunk chunk : chunks) {
                columnChunkPageReadStore.addColumn(chunk.descriptor.colIndex, chunk.readAllPages());
            }
        }
        ++currentBlock;
        ParquetMetrics.get().groupReadEnd();

        return columnChunkPageReadStore;
    }

    /**
     * The data for a column chunk
     *
     * @author Julien Le Dem
     *
     */
    class Chunk extends ByteArrayInputStream {

        protected final ChunkDescriptor descriptor;
        protected long baseOffset = 0;

        /**
         *
         * @param descriptor descriptor for the chunk
         * @param data contains the chunk data at offset
         * @param offset where the chunk starts in offset
         */
        public Chunk(ChunkDescriptor descriptor, byte[] data, int offset) {
            super(data);
            this.descriptor = descriptor;
            this.pos = offset;
        }

        public Chunk(ChunkDescriptor descriptor, byte[] data, int offset, long baseOffset) {
            super(data);
            this.descriptor = descriptor;
            this.pos = offset;
            this.baseOffset = baseOffset;
        }

        protected PageHeader readPageHeader() throws IOException {
            return Util.readPageHeader(this);
        }

        /**
         * Read all of the pages in a given column chunk.
         * @return the list of pages
         */
        public ColumnChunkPageReadStore.ColumnChunkPageReader readAllPages() throws IOException {
            int maxOffset = this.pos + descriptor.size;
            List<Long> pageOffsets = descriptor.pageOffsets;
            List<DataPage> pagesInChunk = new ArrayList<>();
            CompressionCodecName codec = descriptor.metadata.getCodec();
            Decompressor decompressorByName = getDecompressorByName(codec);
            while (this.pos < maxOffset) {
                if (pageOffsets.contains(currentOffset())) {
                    PageHeader pageHeader = readPageHeader();
                    int compressedPageSize = pageHeader.getCompressed_page_size();
                    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
                    switch (pageHeader.type) {
                    case DATA_PAGE:
                        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                        pagesInChunk.add(new DataPageV1(this.readAsBytesInput(compressedPageSize),
                                dataHeaderV1.getNum_values(), uncompressedPageSize,
                                fromParquetStatistics(null, dataHeaderV1.getStatistics(), descriptor.col.getType()),
                                converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                                converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                                converter.getEncoding(dataHeaderV1.getEncoding())));
                        break;
                    case DATA_PAGE_V2:
                        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length()
                                - dataHeaderV2.getDefinition_levels_byte_length();
                        pagesInChunk.add(new DataPageV2(dataHeaderV2.getNum_rows(), dataHeaderV2.getNum_nulls(),
                                dataHeaderV2.getNum_values(),
                                this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                                this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                                converter.getEncoding(dataHeaderV2.getEncoding()), this.readAsBytesInput(dataSize),
                                uncompressedPageSize,
                                fromParquetStatistics(null, dataHeaderV2.getStatistics(), descriptor.col.getType()),
                                dataHeaderV2.isIs_compressed()));
                        break;
                    default:
                        if (DEBUG)
                            logger.debug(
                                    "skipping page of type " + pageHeader.getType() + " of size " + compressedPageSize);
                        this.skip(compressedPageSize);
                        break;
                    }
                } else {
                    PageHeader pageHeader = readPageHeader();
                    int compressedPageSize = pageHeader.getCompressed_page_size();
                    this.skip(compressedPageSize);
                }
            }
            return new ColumnChunkPageReadStore.ColumnChunkPageReader(configuration, codec, decompressorByName,
                    pagesInChunk, null);
        }

        public long currentOffset() {
            return this.pos + baseOffset;
        }

        /**
         * @return the current position in the chunk
         */
        public int pos() {
            return this.pos;
        }

        /**
         * @param size the size of the page
         * @return the page
         * @throws IOException
         */
        public BytesInput readAsBytesInput(int size) throws IOException {
            final BytesInput r = BytesInput.from(this.buf, this.pos, size);
            this.pos += size;
            return r;
        }

    }

    /**
     * deals with a now fixed bug where compressedLength was missing a few bytes.
     *
     * @author Julien Le Dem
     *
     */
    class WorkaroundChunk extends Chunk {

        private final FSDataInputStream f;

        /**
         * @param descriptor the descriptor of the chunk
         * @param data contains the data of the chunk at offset
         * @param offset where the chunk starts in data
         * @param f the file stream positioned at the end of this chunk
         */
        protected WorkaroundChunk(ChunkDescriptor descriptor, byte[] data, int offset, FSDataInputStream f,
                long baseOffset) {
            super(descriptor, data, offset, baseOffset);
            this.f = f;
        }

        protected PageHeader readPageHeader() throws IOException {
            PageHeader pageHeader;
            int initialPos = this.pos;
            try {
                pageHeader = Util.readPageHeader(this);
            } catch (IOException e) {
                // this is to workaround a bug where the compressedLength
                // of the chunk is missing the size of the header of the dictionary
                // to allow reading older files (using dictionary) we need this.
                // usually 13 to 19 bytes are missing
                // if the last page is smaller than this, the page header itself is truncated in the buffer.
                this.pos = initialPos; // resetting the buffer to the position before we got the error
                //            LOG.info("completing the column chunk to read the page header");
                pageHeader = Util.readPageHeader(new SequenceInputStream(this, f)); // trying again from the buffer + remainder of the stream.
            }
            return pageHeader;
        }

        public BytesInput readAsBytesInput(int size) throws IOException {
            if (pos + size > count) {
                // this is to workaround a bug where the compressedLength
                // of the chunk is missing the size of the header of the dictionary
                // to allow reading older files (using dictionary) we need this.
                // usually 13 to 19 bytes are missing
                int l1 = count - pos;
                int l2 = size - l1;
                //            LOG.info("completed the column chunk with " + l2 + " bytes");
                return BytesInput.concat(super.readAsBytesInput(l1), BytesInput.copy(BytesInput.from(f, l2)));
            }
            return super.readAsBytesInput(size);
        }

    }

    /**
     * information needed to read a column chunk
     */
    class ChunkDescriptor {

        protected final ColumnDescriptor col;
        private final ColumnChunkMetaData metadata;
        protected final long fileOffset;
        final int size;
        protected List<Long> pageOffsets;
        private Integer colIndex;

        /**
         * @param col column this chunk is part of
         * @param metadata metadata for the column
         * @param fileOffset offset in the file where this chunk starts
         * @param size size of the chunk
         */
        protected ChunkDescriptor(ColumnDescriptor col, ColumnChunkMetaData metadata, long fileOffset, int size) {
            super();
            this.col = col;
            this.metadata = metadata;
            this.fileOffset = fileOffset;
            this.size = size;
        }

        protected ChunkDescriptor(ColumnDescriptor col, ColumnChunkMetaData metadata, long fileOffset, int size,
                List<Long> pageOffsets, Integer colIndex) {
            super();
            this.col = col;
            this.metadata = metadata;
            this.fileOffset = fileOffset;
            this.size = size;
            this.pageOffsets = pageOffsets;
            this.colIndex = colIndex;
        }

    }

    /**
     * describes a list of consecutive column chunks to be read at once.
     *
     * @author Julien Le Dem
     */
    class ConsecutiveChunkList {

        private final long offset;
        private int length;
        private final List<ChunkDescriptor> chunks = new ArrayList<ChunkDescriptor>();

        /**
         * @param offset where the first chunk starts
         */
        ConsecutiveChunkList(long offset) {
            this.offset = offset;
        }

        /**
         * adds a chunk to the list.
         * It must be consecutive to the previous chunk
         * @param descriptor
         */
        public void addChunk(ChunkDescriptor descriptor) {
            chunks.add(descriptor);
            length += descriptor.size;
        }

        /**
         * @param f file to read the chunks from
         * @return the chunks
         * @throws IOException
         */
        public List<Chunk> readAll(FSDataInputStream f) throws IOException {
            List<Chunk> result = new ArrayList<>(chunks.size());
            f.seek(offset);
            byte[] chunksBytes = new byte[length];
            f.readFully(chunksBytes);
            // report in a counter the data we just scanned
            BenchmarkCounter.incrementBytesRead(length);
            int currentChunkOffset = 0;
            for (int i = 0; i < chunks.size(); i++) {
                ChunkDescriptor descriptor = chunks.get(i);
                if (i < chunks.size() - 1) {
                    result.add(new Chunk(descriptor, chunksBytes, currentChunkOffset, offset));
                } else {
                    // because of a bug, the last chunk might be larger than descriptor.size
                    result.add(new WorkaroundChunk(descriptor, chunksBytes, currentChunkOffset, f, offset));
                }
                currentChunkOffset += descriptor.size;
            }
            return result;
        }

        /**
         * @return the position following the last byte of these chunks
         */
        public long endPos() {
            return offset + length;
        }

    }
}
