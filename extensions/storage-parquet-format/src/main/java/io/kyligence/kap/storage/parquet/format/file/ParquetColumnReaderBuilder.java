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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ParquetColumnReaderBuilder {
    private Configuration conf = null;
    private ParquetMetadata metadata = null;
    private Path path = null;
    private int column = 0;
    private ImmutableRoaringBitmap pageBitset = null;
    private long fileOffset;

    public ParquetColumnReaderBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetColumnReaderBuilder setMetadata(ParquetMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public ParquetColumnReaderBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetColumnReaderBuilder setColumn(int column) {
        this.column = column;
        return this;
    }

    public ParquetColumnReaderBuilder setPageBitset(ImmutableRoaringBitmap bitset) {
        this.pageBitset = bitset;
        return this;
    }

    public ParquetColumnReaderBuilder setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
        return this;
    }

    public ParquetColumnReader build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }

        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        return new ParquetColumnReader(new ParquetRawReader(conf, path, metadata, fileOffset), column, pageBitset);
    }
}
