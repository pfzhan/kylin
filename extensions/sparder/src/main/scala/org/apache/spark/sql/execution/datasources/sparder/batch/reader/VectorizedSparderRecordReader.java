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
import java.util.Map;

import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ExpandableBytesVector;

public class VectorizedSparderRecordReader {

    private SparderFileReader sparderFileReader;
    private ColumnarBatch columnarBatch;
    private ColumnChunkPageReadStore columnChunkPageReadStore;
    public Map<Integer, VectorizedSparderColumnReader> columnReaders;
    private int[] columns;
    private boolean init = false;

    public VectorizedSparderRecordReader(SparderFileReader sparderFileReader, ImmutableRoaringBitmap columns,
            ColumnarBatch columnarBatch) {
        this.sparderFileReader = sparderFileReader;
        this.columnarBatch = columnarBatch;
        this.columns = columns.toArray();
        this.columnReaders = Maps.newHashMap();
    }

    public void nextBatch() throws IOException {
         columnChunkPageReadStore = sparderFileReader.readNextRowGroup();
        if (columnChunkPageReadStore != null) {
            readGroup();
        }
    }

    public void readGroup() {
        for (int i = 0; i < columns.length; ++i) {
            columnReaders.put(columns[i], new VectorizedSparderColumnReader(
                    columnChunkPageReadStore.getPageReader(columns[i]),
                    sparderFileReader.parquetMetadata.getFileMetaData().getSchema().getColumns().get(columns[i])));
        }
    }

    public ExpandableBytesVector nextPage(int column) throws IOException {
        if (!init) {
            init = true;
            nextBatch();
        }
        VectorizedSparderColumnReader vectorizedSparderColumnReader = columnReaders.get(column);
        if (vectorizedSparderColumnReader == null)
            return null;
        return vectorizedSparderColumnReader.readNextPage();
    }

    public void close() throws IOException {
        sparderFileReader.close();
    }
}