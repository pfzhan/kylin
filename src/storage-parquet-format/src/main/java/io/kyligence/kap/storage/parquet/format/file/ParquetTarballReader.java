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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetTarballReader extends ParquetRawReader {
    private long skipLength = 0;

    public ParquetTarballReader(Configuration config, Path path, ParquetMetadata metadata, ParquetMetrics metrics) throws IOException {
        super(config, path, metadata, metrics, 0);
        skipLength = getSkipOffset(config, path);
    }

    public GeneralValuesReader getValuesReader(int globalPageIndex, int column) throws IOException {
        int group = globalPageIndex / pagesPerGroup;
        int page = globalPageIndex % pagesPerGroup;
        if (!indexMap.containsKey(group + "," + column + "," + page)) {
            return null;
        }
        long offset = Long.parseLong(indexMap.get(group + "," + column + "," + page));
        return getValuesReaderFromOffset(group, column, offset + skipLength);
    }

    public GeneralValuesReader getValuesReader(int rowGroup, int column, int pageIndex) throws IOException {
        long pageOffset = Long.parseLong(indexMap.get(rowGroup + "," + column + "," + pageIndex));
        return getValuesReaderFromOffset(rowGroup, column, pageOffset + skipLength);
    }

    private long getSkipOffset(Configuration config, Path path) throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem(path, config);
        FSDataInputStream in = fs.open(path);
        skipLength = in.readLong();
        in.close();
        return skipLength;
    }
}
