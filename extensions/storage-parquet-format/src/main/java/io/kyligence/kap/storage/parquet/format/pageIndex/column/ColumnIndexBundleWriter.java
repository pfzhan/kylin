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

package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ColumnIndexBundleWriter implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(ColumnIndexBundleWriter.class);

    private int columnNum;
    private ColumnIndexWriter[] indexWriters;
    private File localIndexDir;
    private ColumnSpec[] columnSpecs;
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();

    public ColumnIndexBundleWriter(ColumnSpec[] columnSpecs, File localIndexDir) throws IOException {
        this.columnNum = columnSpecs.length;
        this.columnSpecs = columnSpecs;
        this.localIndexDir = localIndexDir;
        this.indexWriters = new ColumnIndexWriter[columnNum];
        for (int col = 0; col < columnNum; col++) {
            File indexFile = getColumnIndexFile(col);
            indexWriters[col] = new ColumnIndexWriter(columnSpecs[col], new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile), kapConfig.getParquetPageIndexIOBufSize())));
        }
    }

    public void write(byte[] rowKey, int docId) {
        write(rowKey, 0, docId);
    }

    public void write(byte[] rowKey, int startOffset, int docId) {
        int columnOffset = startOffset;
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].appendToRow(new ByteArray(rowKey, columnOffset, columnSpecs[i].getColumnLength()), docId);
            columnOffset += columnSpecs[i].getColumnLength();
        }
    }

    public void write(List<byte[]> rowKeys, int docId) {
        Preconditions.checkState(rowKeys.size() == columnNum);

        for (int i = 0; i < columnNum; i++) {
            byte[] currRowKey = rowKeys.get(i);
            indexWriters[i].appendToRow(new ByteArray(currRowKey), docId);
        }
    }

    public void spill() {
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].spill();
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].close();
        }
    }

    public File getColumnIndexFile(int col) {
        return new File(localIndexDir, Integer.toString(columnSpecs[col].getColumnSequence()) + ".inv");
    }

    public long getIndexSizeInBytes(int col) {
        return indexWriters[col].getTotalSize();
    }
}
