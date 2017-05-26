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
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.Pair;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexReader;

public class ParquetPageIndexReader implements Closeable {
    private ColumnIndexReader[] columnIndexReaders;
    private int columnNum;
    private long[] startOffsets;
    private FSDataInputStream inputStream;
    private Pair<Integer, Integer> pageRange;

    private int lastestUsedColumn = -1;

    public ParquetPageIndexReader(FSDataInputStream inputStream, long startOffset) throws IOException {
        this(inputStream, startOffset, null);
    }

    public ParquetPageIndexReader(FSDataInputStream inputStream, long startOffset, Pair<Integer, Integer> pageRange) throws IOException{
        this.inputStream = inputStream;
        inputStream.seek(startOffset);
        this.columnNum = inputStream.readInt();
        this.columnIndexReaders = new ColumnIndexReader[columnNum];
        this.startOffsets = new long[columnNum];

        for (int i = 0; i < columnNum; i++) {
            startOffsets[i] = inputStream.readLong();
        }

        for (int i = 0; i < columnNum; i++) {
            columnIndexReaders[i] = new ColumnIndexReader(inputStream, startOffsets[i] + startOffset);
        }

        this.pageRange = pageRange;
    }

    @Override
    public void close() throws IOException {
        closeWithoutStream();
        inputStream.close();
    }

    public void closeWithoutStream() throws IOException {
        for (ColumnIndexReader columnIndexReader : columnIndexReaders) {
            columnIndexReader.close();
        }
    }

    public ColumnIndexReader readColumnIndex(int col) {
        lastestUsedColumn = col;
        return columnIndexReaders[col];
    }

    public int getLastestUsedColumn() {
        return lastestUsedColumn;
    }

    public int getPageTotalNum(int col) {
        return columnIndexReaders[col].getPageNum();
    }

    public Pair<Integer, Integer> getPageRange() {
        return pageRange;
    }
}
