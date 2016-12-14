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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetBundleReader {
    public static final Logger logger = LoggerFactory.getLogger(ParquetBundleReader.class);

    private List<ParquetReaderState> readerStates;


    public ParquetBundleReader(Configuration configuration, Path path, ImmutableRoaringBitmap columns, ImmutableRoaringBitmap pageBitset, long fileOffset, ParquetMetadata metadata) throws IOException {
        readerStates = new ArrayList<>(columns.getCardinality());

        if (metadata == null) {
            metadata = ParquetFileReader.readFooter(configuration, path, ParquetMetadataConverter.NO_FILTER);
        }
        for (int column : columns) {
            readerStates.add(new ParquetReaderState(new ParquetColumnReader.Builder().setFileOffset(fileOffset).setConf(configuration).setPath(path).setColumn(column).setPageBitset(pageBitset).setMetadata(metadata).build()));
            logger.info("Read Column: " + column);
        }
    }

    public List<Object> read() throws IOException {
        List<Object> result = new ArrayList<Object>();
        for (ParquetReaderState state : readerStates) {
            Object value = state.cache.poll();
            if (value == null) {
                GeneralValuesReader curReader;
                if ((curReader = state.getNextValuesReader()) == null) {
                    return null;
                }

                // create cache
                Object o = null;
                state.cache.clear();
                while ((o = curReader.readData()) != null) {
                    state.cache.add(o);
                }

                value = state.cache.poll();
                if (value == null) {
                    return null;
                }
            }
            result.add(value);
        }
        return result;
    }

    public int getPageIndex() {
        return readerStates.get(0).reader.getPageIndex();
    }

    public void close() throws IOException {
        for (ParquetReaderState state : readerStates) {
            state.reader.close();
        }
    }

    private class ParquetReaderState {
        private ParquetColumnReader reader;
        private Queue<Object> cache;

        public ParquetReaderState(ParquetColumnReader reader) throws IOException {
            this.reader = reader;
            cache = new ArrayDeque<>();
        }

        public GeneralValuesReader getNextValuesReader() throws IOException {
            return reader.getNextValuesReader();
        }

        public void setReader(ParquetColumnReader reader) {
            this.reader = reader;
        }
    }

    public static class Builder {
        private Configuration conf;
        private Path path;
        private ImmutableRoaringBitmap columnBitset = null;
        private ImmutableRoaringBitmap pageBitset = null;
        private long fileOffset = 0;

        public Builder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }

        public Builder setColumnsBitmap(ImmutableRoaringBitmap columns) {
            this.columnBitset = columns;
            return this;
        }

        public Builder setPageBitset(ImmutableRoaringBitmap bitset) {
            this.pageBitset = bitset;
            return this;
        }

        public Builder setFileOffset(long fileOffset) {
            this.fileOffset = fileOffset;
            return this;
        }

        public ParquetBundleReader build() throws IOException {
            if (conf == null) {
                throw new IllegalStateException("Configuration should be set");
            }

            if (path == null) {
                throw new IllegalStateException("Output file path should be set");
            }
            if (columnBitset == null) {
                int columnCnt = new ParquetRawReader.Builder().setConf(conf).setPath(path).build().getColumnCount();
                columnBitset = createBitset(columnCnt);
            }

            ParquetBundleReader result = new ParquetBundleReader(conf, path, columnBitset, pageBitset, fileOffset, null);

            return result;
        }

        private static ImmutableRoaringBitmap createBitset(int total) throws IOException {
            MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
            for (int i = 0; i < total; ++i) {
                mBitmap.add(i);
            }

            ImmutableRoaringBitmap iBitmap = null;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos);) {
                mBitmap.serialize(dos);
                dos.flush();
                iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
            }

            return iBitmap;
        }

        /* This function is for test */
        public static void main(String[] args) {
            if (args.length < 1) {
                System.out.println("Need a file name");
                return;
            }

            System.out.println("Read file " + args[0]);

            long t = System.currentTimeMillis();
            try {
                int i = 0;
                ParquetBundleReader reader = new Builder().setPath(new Path(args[0])).setConf(new Configuration()).build();
                long t2 = System.currentTimeMillis() - t;
                System.out.println("Create reader takes " + t2 + " ms");
                while (reader.read() != null) {
                    i++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            t = System.currentTimeMillis() - t;

            System.out.println("Read file takes " + t + " ms");
        }
    }
}
