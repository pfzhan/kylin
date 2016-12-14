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
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class ParquetSpliceReader {
    private long fileOffset;
    private ParquetBundleReader reader;
    private Map<String, Pair<Integer, Integer>> divCache;
    private Configuration configuration;
    private Path path;
    private ImmutableRoaringBitmap columns;
    private ParquetMetadata metadata;

    public ParquetSpliceReader(Configuration configuration, Path path, ImmutableRoaringBitmap columns, long fileOffset) throws IOException {
        this.configuration = configuration;
        this.path = path;
        this.columns = columns;
        this.fileOffset = fileOffset;
        metadata = ParquetFileReader.readFooter(configuration, path, ParquetMetadataConverter.NO_FILTER);
        divCache = DivisionUtils.filterDivision(metadata.getFileMetaData().getKeyValueMetaData());
    }

    public Set<String> getDivs() {
        if (divCache == null) {
            return null;
        }
        return divCache.keySet();
    }

    /**
     * get bundle reader for one division, it's a full data reader
     * @param div Div name
     * @return the bundle reader
     */
    public ParquetBundleReader getDivReader(String div) throws IOException {
        Pair<Integer, Integer> range = divCache.get(div);
        range.getLeft();
        range.getRight();
        return new ParquetBundleReader(configuration, path, columns, Builder.createBitset(range.getLeft(), range.getRight()), fileOffset, metadata);
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

        public ParquetSpliceReader build() throws IOException {
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

            return new ParquetSpliceReader(conf, path, columnBitset, fileOffset);

        }

        private static ImmutableRoaringBitmap createBitset(int begin, int end) throws IOException {
            MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
            for (int i = begin; i < end; ++i) {
                mBitmap.add(i);
            }

            ImmutableRoaringBitmap iBitmap;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
                mBitmap.serialize(dos);
                dos.flush();
                iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
            }

            return iBitmap;
        }

        private static ImmutableRoaringBitmap createBitset(int total) throws IOException {
            return createBitset(0, total);
        }
    }
}
