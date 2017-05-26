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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.Pair;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Lists;

public class ParquetPageIndexSpliceReader {
    private FSDataInputStream inputStream;
    private long startOffset;
    private ParquetPageIndexSpliceMeta cuboidMeta;

    /**
     * Parquet index file splice reader
     * @param inputStream input stream to read data
     * @param fileSize file offset counting from file beginning to the end of index, tail after index not considered
     * @param startOffset start offset counting from file beginning to index
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public ParquetPageIndexSpliceReader(FSDataInputStream inputStream, long fileSize, long startOffset) throws IOException, ClassNotFoundException {
        this.inputStream = inputStream;
        this.startOffset = startOffset;

        inputStream.seek(fileSize - 8);
        inputStream.seek(inputStream.readLong() + startOffset);
        ObjectInputStream ois = new ObjectInputStream(inputStream);
        cuboidMeta = (ParquetPageIndexSpliceMeta) ois.readObject();
    }

    public List<ParquetPageIndexReader> getIndexReaderByCuboid(Long cuboid) throws IOException {
        List<ParquetPageIndexReader> readerList = Lists.newArrayList();
        for (ParquetPageIndexSpliceMeta.CuboidSpliceInfo offsetInfo: cuboidMeta.get(cuboid)) {
            readerList.add(new ParquetPageIndexReader(inputStream, offsetInfo.getDivOffset() + startOffset, new Pair<>(offsetInfo.getDivPageRangeStart(), offsetInfo.getDivPageRangeEnd())));
        }
        return readerList;
    }

    public ImmutableRoaringBitmap getFullBitmap(long cuboid) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (ParquetPageIndexSpliceMeta.CuboidSpliceInfo pageRangeInfo: this.cuboidMeta.get(cuboid)) {
            bitmap.add(pageRangeInfo.getDivPageRangeStart(), pageRangeInfo.getDivPageRangeEnd());
        }
        return bitmap.toImmutableRoaringBitmap();
    }
}
