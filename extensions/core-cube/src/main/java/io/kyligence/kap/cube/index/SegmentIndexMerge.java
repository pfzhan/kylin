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
