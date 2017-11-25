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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.storage.parquet.format.file.ParquetMetrics;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.Ints;
import org.apache.parquet.Log;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ParquetDecodingException;

import io.kyligence.kap.storage.parquet.format.file.CodecFactory;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 *
 */
class ColumnChunkPageReadStore {
    private static final Log LOG = Log.getLog(ColumnChunkPageReadStore.class);

    /**
     * PageReader for a single column chunk. A column chunk contains
     * several pages, which are yielded one by one in order.
     *
     * This implementation is provided with a list of pages, each of which
     * is decompressed and passed through.
     */
    static final class ColumnChunkPageReader implements PageReader {

        private final Decompressor decompressor;
        private final long valueCount;
        private final List<DataPage> compressedPages;
        private final DictionaryPage compressedDictionaryPage;
        private final CompressionCodecName codec;
        private final Configuration configuration;

        ColumnChunkPageReader(Configuration configuration, CompressionCodecName codec, Decompressor decompressor,
                List<DataPage> compressedPages, DictionaryPage compressedDictionaryPage) {
            this.decompressor = decompressor;
            this.configuration = configuration;
            this.compressedPages = new LinkedList<DataPage>(compressedPages);
            this.compressedDictionaryPage = compressedDictionaryPage;
            this.codec = codec;
            int count = 0;
            for (DataPage p : compressedPages) {
                count += p.getValueCount();
            }
            this.valueCount = count;
        }

        @Override
        public long getTotalValueCount() {
            return valueCount;
        }

        @Override
        public DataPage readPage() {
            if (compressedPages.isEmpty()) {
                return null;
            }
            DataPage compressedPage = compressedPages.remove(0);
            return compressedPage.accept(new DataPage.Visitor<DataPage>() {
                @Override
                public DataPage visit(DataPageV1 dataPageV1) {
                    try {
                        return new DataPageV1(decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize()),
                                dataPageV1.getValueCount(), dataPageV1.getUncompressedSize(),
                                dataPageV1.getStatistics(), dataPageV1.getRlEncoding(), dataPageV1.getDlEncoding(),
                                dataPageV1.getValueEncoding());
                    } catch (IOException e) {
                        throw new ParquetDecodingException("could not decompress page", e);
                    }
                }

                @Override
                public DataPage visit(DataPageV2 dataPageV2) {
                    if (!dataPageV2.isCompressed()) {
                        return dataPageV2;
                    }
                    try {
                        int uncompressedSize = Ints.checkedCast(dataPageV2.getUncompressedSize()
                                - dataPageV2.getDefinitionLevels().size() - dataPageV2.getRepetitionLevels().size());
                        return DataPageV2.uncompressed(dataPageV2.getRowCount(), dataPageV2.getNullCount(),
                                dataPageV2.getValueCount(), dataPageV2.getRepetitionLevels(),
                                dataPageV2.getDefinitionLevels(), dataPageV2.getDataEncoding(),
                                decompress(dataPageV2.getData(), uncompressedSize), dataPageV2.getStatistics());
                    } catch (IOException e) {
                        throw new ParquetDecodingException("could not decompress page", e);
                    }
                }
            });
        }

        @Override
        public DictionaryPage readDictionaryPage() {
            if (compressedDictionaryPage == null) {
                return null;
            }
            try {
                return new DictionaryPage(
                        decompress(compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize()),
                        compressedDictionaryPage.getDictionarySize(), compressedDictionaryPage.getEncoding());
            } catch (IOException e) {
                throw new RuntimeException(e); // TODO: cleanup
            }
        }

        public BytesInput decompress(BytesInput bytesInput, int uncompressedSize) throws IOException {
            ParquetMetrics.get().pageReadIOAndDecompressStart();

            byte[] ret = new byte[uncompressedSize];
            CompressionCodec compressionCodec = CodecFactory.getCodec(codec, configuration);
            InputStream is = bytesInput.toInputStream() ;
            if(compressionCodec != null){
                is = compressionCodec.createInputStream(bytesInput.toInputStream(), decompressor);
            }
            IOUtils.readFully(is, ret, 0, uncompressedSize);
            ParquetMetrics.get().pageReadIOAndDecompressEnd(bytesInput.size(), uncompressedSize);

            return BytesInput.from(ret);
        }
    }

    private final Map<Integer, ColumnChunkPageReader> readers = new HashMap<Integer, ColumnChunkPageReader>();
    private final long rowCount;

    public ColumnChunkPageReadStore(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public PageReader getPageReader(Integer colIndex) {
        if (!readers.containsKey(colIndex)) {
            throw new IllegalArgumentException(colIndex + " is not in the store: " + readers.keySet() + " " + rowCount);
        }
        return readers.get(colIndex);
    }

    void addColumn(Integer colIndex, ColumnChunkPageReader reader) {
        if (readers.put(colIndex, reader) != null) {
            throw new RuntimeException(colIndex + " was added twice");
        }
    }

}
