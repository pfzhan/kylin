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

import java.io.File;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dict.DateStrDictionary;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * translate a forward index table
 */
public class SegmentIndexReEncode implements Iterator<ByteArray> {

    private Dictionary[] oldDictionaries;
    private Dictionary[] newDictionaries;
    private int colNeedIndex;
    private IColumnForwardIndex.Reader[] columnReaders;
    int totalLength;
    int cursor;
    int totalRows;
    private ByteArray reuseByteArray;

    public SegmentIndexReEncode(CubeSegment oldSegment, CubeSegment newSegment, int colNeedIndex, File localTempFolder) {
        this.colNeedIndex = colNeedIndex;
        columnReaders = new IColumnForwardIndex.Reader[colNeedIndex];
        final long baseCuboidId = Cuboid.getBaseCuboidId(newSegment.getCubeDesc());
        final Cuboid baseCuboid = Cuboid.findById(newSegment.getCubeDesc(), baseCuboidId);
        oldDictionaries = new Dictionary[colNeedIndex];
        newDictionaries = new Dictionary[colNeedIndex];
        for (int j = 0; j < colNeedIndex; j++) {
            TblColRef column = baseCuboid.getColumns().get(j);
            oldDictionaries[j] = oldSegment.getDictionary(column);
            newDictionaries[j] = newSegment.getDictionary(column);
            totalLength += newDictionaries[j].getSizeOfId();
            int maxValue = oldDictionaries[j].getMaxId();
            if (oldDictionaries[j] instanceof DateStrDictionary) {
                maxValue = maxValue / 4;
            }
            columnReaders[j] = ColumnIndexFactory.createLocalForwardIndex(column.getName(), maxValue, new File(new File(localTempFolder, oldSegment.getName()), column.getName() + ".fwd").getAbsolutePath()).getReader();
        }

        totalRows = columnReaders[0].getNumberOfRows();
        cursor = 0;
        reuseByteArray = new ByteArray(totalLength);
    }

    byte[] getColumn(int row, int column) {
        int v = columnReaders[column].get(row);
        return oldDictionaries[column].getValueBytesFromId(v);
    }

    @Override
    public boolean hasNext() {
        return cursor < totalRows;
    }

    @Override
    public ByteArray next() {
        ByteArray value = translateRow(cursor);
        cursor++;
        return value;
    }

    private ByteArray translateRow(int row) {
        int offset = 0;
        for (int i = 0; i < colNeedIndex; i++) {
            byte[] oldValue = getColumn(row, i);
            int newId = newDictionaries[i].getIdFromValueBytes(oldValue, 0, oldValue.length);
            BytesUtil.writeUnsigned(newId, reuseByteArray.array(), offset, newDictionaries[i].getSizeOfId());
            offset += newDictionaries[i].getSizeOfId();
        }

        return reuseByteArray;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
