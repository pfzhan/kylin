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

package io.kyligence.kap.cube.index.pinot;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.pinot.util.MmapUtils;
import io.kyligence.kap.cube.index.pinot.util.Pairs.IntPair;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 * 
 * Aug 10, 2014
 */
public class BitmapInvertedIndexReader implements InvertedIndexReader {
    public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

    private int numberOfBitmaps;
    private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> bitmaps = null;

    private RandomAccessFile _rndFile;
    private ByteBuffer buffer;
    public static final int INT_SIZE_IN_BYTES = Integer.SIZE / Byte.SIZE;

    private File file;

    /**
     * Constructs an inverted index with the specified size.
     * @param cardinality the number of bitmaps in the inverted index, which should be the same as the
     *          number of values in
     *          the dictionary.
     * @throws IOException
     */
    public BitmapInvertedIndexReader(File file, boolean isMmap) throws IOException {
        this.file = file;
        load(file, isMmap);
    }

    /**
     * {@inheritDoc}
     * @see InvertedIndexReader#getImmutable(int)
     */
    @Override
    public ImmutableRoaringBitmap getImmutable(int idx) {
        SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
        // Return the bitmap if it's still on heap
        if (bitmaps != null) {
            bitmapArrayReference = bitmaps.get();
            if (bitmapArrayReference != null) {
                SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[idx];
                if (bitmapReference != null) {
                    ImmutableRoaringBitmap value = bitmapReference.get();
                    if (value != null) {
                        return value;
                    }
                }
            } else {
                bitmapArrayReference = new SoftReference[numberOfBitmaps];
                bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
            }
        } else {
            bitmapArrayReference = new SoftReference[numberOfBitmaps];
            bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
        }
        synchronized (this) {
            ImmutableRoaringBitmap value;
            if (bitmapArrayReference[idx] == null || bitmapArrayReference[idx].get() == null) {
                value = buildRoaringBitmapForIndex(idx);
                bitmapArrayReference[idx] = new SoftReference<ImmutableRoaringBitmap>(value);
            } else {
                value = bitmapArrayReference[idx].get();
            }
            return value;
        }

    }

    public int getNumberOfRows() {
        return numberOfBitmaps;
    }

    private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int index) {
        final int currentOffset = getOffset(index);
        final int nextOffset = getOffset(index + 1);
        final int bufferLength = nextOffset - currentOffset;

        // Slice the buffer appropriately for Roaring Bitmap
        buffer.position(currentOffset);
        final ByteBuffer bb = buffer.slice();
        bb.limit(bufferLength);

        ImmutableRoaringBitmap immutableRoaringBitmap = null;
        try {
            immutableRoaringBitmap = new ImmutableRoaringBitmap(bb);
        } catch (Exception e) {
            LOGGER.error("Error creating immutableRoaringBitmap for dictionary id:{} currentOffset:{} bufferLength:{} slice position{} limit:{} file:{}", index, currentOffset, bufferLength, bb.position(), bb.limit(), file.getAbsolutePath());
        }
        return immutableRoaringBitmap;
    }

    private int getOffset(final int index) {
        return buffer.getInt(index * INT_SIZE_IN_BYTES);
    }

    private void load(File file, boolean isMmap) throws IOException {
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        numberOfBitmaps = dis.readInt();
        dis.skipBytes(numberOfBitmaps * INT_SIZE_IN_BYTES);
        final int lastOffset = dis.readInt();
        dis.close();

        _rndFile = new RandomAccessFile(file, "r");
        if (isMmap) {
            buffer = MmapUtils.mmapFile(_rndFile, MapMode.READ_ONLY, 0, lastOffset, file, this.getClass().getSimpleName() + " buffer");
        } else {
            buffer = MmapUtils.allocateDirectByteBuffer(lastOffset, file, this.getClass().getSimpleName() + " buffer");
            _rndFile.getChannel().read(buffer, INT_SIZE_IN_BYTES);
        }
    }

    @Override
    public void close() throws IOException {
        MmapUtils.unloadByteBuffer(buffer);
        buffer = null;
        if (_rndFile != null) {
            _rndFile.close();
        }
    }

    @Override
    public IntPair getMinMaxRangeFor(int dicId) {
        throw new UnsupportedOperationException("not supported in inverted index type bitmap");
    }
}
