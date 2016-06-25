/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
