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
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 */
public class SegmentIndexMerge {

    private final CubeSegment newSeg;
    private final List<CubeSegment> mergingSegments;
    private final File localTempFolder;

    public SegmentIndexMerge(CubeSegment newSeg, List<CubeSegment> mergingSegments, File localTempFolder) {
        this.newSeg = newSeg;
        this.mergingSegments = mergingSegments;
        this.localTempFolder = localTempFolder;
    }

    public List<File> mergeIndex() throws IOException {
        final int[] columnsNeedIndex = newSeg.getCubeDesc().getRowkey().getColumnsNeedIndex();
        final int colNeedIndex = columnsNeedIndex.length;
        final long baseCuboidId = Cuboid.getBaseCuboidId(newSeg.getCubeDesc());
        final Cuboid baseCuboid = Cuboid.findById(newSeg.getCubeDesc(), baseCuboidId);
        File newSegFolder = new File(localTempFolder, newSeg.getName());
        newSegFolder.mkdirs();

        final int numberOfSegments = mergingSegments.size();
        List<SegmentIndexReEncode> translators = Lists.newArrayList();
        for (int i = 0; i < numberOfSegments; i++) {
            translators.add(new SegmentIndexReEncode(mergingSegments.get(i), newSeg, colNeedIndex, localTempFolder));
        }

        Iterator<ByteArray> merged = Iterators.mergeSorted(translators, new Comparator<ByteArray>() {

            @Override
            public int compare(ByteArray o1, ByteArray o2) {
                return o1.compareTo(o2);
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }
        });

        RowKeyColumnIO colIO = new RowKeyColumnIO(new CubeDimEncMap(newSeg));
        ColumnIndexWriter[] columnIndexWriters = new ColumnIndexWriter[colNeedIndex];
        int offset = 0;
        List<File> localFiles = Lists.newArrayList();
        for (int i = 0; i < colNeedIndex; i++) {
            TblColRef col = baseCuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(col);
            File fwdFile = new File(newSegFolder, col.getName() + ".fwd");
            File invFile = new File(newSegFolder, col.getName() + ".inv");
            columnIndexWriters[i] = new ColumnIndexWriter(col, newSeg.getDictionary(col), offset, colLength, fwdFile, invFile);
            localFiles.add(fwdFile);
            localFiles.add(invFile);
            offset += colLength;
        }

        ByteArray lastInserted = null;
        while (merged.hasNext()) {
            ByteArray byteArray = merged.next();
            if (lastInserted == null || !lastInserted.equals(byteArray)) {

                Preconditions.checkState(lastInserted == null || lastInserted.compareTo(byteArray) < 0); // in ascending
                //write
                for (int i = 0; i < colNeedIndex; i++) {
                    columnIndexWriters[i].write(byteArray.array(), byteArray.offset(), byteArray.length());
                }

                // update lastInserted
                if (lastInserted == null) {
                    lastInserted = byteArray.copy();
                } else {
                    System.arraycopy(byteArray.array(), byteArray.offset(), lastInserted.array(), 0, byteArray.length());
                }
            }
        }
        for (int i = 0; i < colNeedIndex; i++) {
            IOUtils.closeQuietly(columnIndexWriters[i]);
        }

        return localFiles;
    }
}
