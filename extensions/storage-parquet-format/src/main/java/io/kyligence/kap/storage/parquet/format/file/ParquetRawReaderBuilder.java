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

public class ParquetRawReaderBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf = null;
    private Path path = null;
    private Path indexPath = null;
    private int fileOffset = 0;//if it's a tarball fileoffset is not 0

    public ParquetRawReaderBuilder setIndexPathSuffix(String indexPathSuffix) {
        this.indexPathSuffix = indexPathSuffix;
        return this;
    }

    public ParquetRawReaderBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetRawReaderBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetRawReaderBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetRawReaderBuilder setFileOffset(int fileOffset) {
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

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        return new ParquetRawReader(conf, path, indexPath, fileOffset);
    }
}
