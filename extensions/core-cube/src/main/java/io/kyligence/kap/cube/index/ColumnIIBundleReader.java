/**
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

package io.kyligence.kap.cube.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.util.BytesUtil;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ColumnIIBundleReader implements Closeable {
    private File localIdxDir;
    private int columnNum = 0;
    private IColumnInvertedIndex.Reader[] indexReader;

    public ColumnIIBundleReader(String[] columnName, int[] columnLength, int[] cardinality, File localIdxDir) {
        this.columnNum = columnLength.length;
        this.localIdxDir = localIdxDir;
        this.indexReader = new IColumnInvertedIndex.Reader[columnNum];
        for (int col = 0; col < columnNum; col++) {
            indexReader[col] = buildColumnIIReader(columnName[col], cardinality[col]);
        }
    }

    private IColumnInvertedIndex.Reader buildColumnIIReader(String colName, int cardinality) {
        IColumnInvertedIndex ii = ColumnIndexFactory.createLocalInvertedIndex(colName, cardinality, new File(localIdxDir, colName + ".inv").getAbsolutePath());
        return ii.getReader();
    }

    public ImmutableRoaringBitmap getRows(int column, int value) {
        return indexReader[column].getRows(value);
    }

    public ImmutableRoaringBitmap getRows(int column, byte[] value) {
        int intVal = BytesUtil.readUnsigned(value, 0, value.length);
        return getRows(column, intVal);
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnNum; i++) {
            indexReader[i].close();
        }
    }
}
