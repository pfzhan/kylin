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

package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexBundleWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;

public class ParquetPageIndexWriter implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexWriter.class);

    private ColumnIndexBundleWriter indexWriter;
    private File tempLocalDir;
    private int columnNum;
    private DataOutputStream outputStream;
    private boolean needSpill;

    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    private final double spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

    private long curOffset = 0;

    public ParquetPageIndexWriter(String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex, DataOutputStream outputStream) throws IOException {
        this(columnNames, columnLength, cardinality, onlyEQIndex, outputStream, true);
    }

    public ParquetPageIndexWriter(String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex, DataOutputStream outputStream, boolean needSpill) throws IOException {
        this.columnNum = columnNames.length;
        this.outputStream = outputStream;
        this.needSpill = needSpill;
        ColumnSpec[] columnSpecs = new ColumnSpec[columnNum];
        for (int i = 0; i < columnSpecs.length; i++) {
            columnSpecs[i] = new ColumnSpec(columnNames[i], columnLength[i], cardinality[i], onlyEQIndex[i], i);
        }
        tempLocalDir = Files.createTempDir();
        indexWriter = new ColumnIndexBundleWriter(columnSpecs, tempLocalDir);
    }

    public ParquetPageIndexWriter(ColumnSpec[] columnSpecs, DataOutputStream outputStream) throws IOException {
        this(columnSpecs, outputStream, true);
    }

    public ParquetPageIndexWriter(ColumnSpec[] columnSpecs, DataOutputStream outputStream, boolean needSpill) throws IOException {
        this.columnNum = columnSpecs.length;
        this.outputStream = outputStream;
        this.needSpill = needSpill;
        tempLocalDir = Files.createTempDir();
        indexWriter = new ColumnIndexBundleWriter(columnSpecs, tempLocalDir);
    }

    public long getCurOffset() {
        return curOffset;
    }

    public void write(byte[] rowKey, int pageId) {
        indexWriter.write(rowKey, 0, pageId);
        spillIfNeeded();
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        indexWriter.write(rowKey, startOffset, pageId);
        spillIfNeeded();
    }

    public void write(List<byte[]> rowKeys, int pageId) {
        indexWriter.write(rowKeys, pageId);
        spillIfNeeded();
    }

    @Override
    public void close() throws IOException {
        closeWithoutStream();
        outputStream.close();
    }

    public void closeWithoutStream() throws IOException {
        indexWriter.close();
        flush();
    }

    private void flush() throws IOException {
        try {
            // write offsets of each column
            outputStream.writeInt(columnNum);
            curOffset += 4;
            long columnIndexOffset = 4 /* Int size */ + columnNum * 8 /* Long size */;
            outputStream.writeLong(columnIndexOffset);
            curOffset += 8;
            for (int i = 0; i < columnNum - 1; i++) {
                columnIndexOffset += indexWriter.getIndexSizeInBytes(i);
                outputStream.writeLong(columnIndexOffset);
                curOffset += 8;
            }

            // write index to output
            for (int i = 0; i < columnNum; i++) {
                File indexFile = indexWriter.getColumnIndexFile(i);
                curOffset += FileUtils.copyFile(indexFile, outputStream);
            }
        } finally {
            FileUtils.forceDelete(tempLocalDir);
        }
    }

    private void spillIfNeeded() {
        if (needSpill) {
            long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
            if (availMemoryMB < spillThresholdMB) {
                logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
                spill();
                logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
            }
        }
    }

    public void spill() {
        indexWriter.spill();
    }
}
