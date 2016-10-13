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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 * 
 * Nov 12, 2014
 */
public class HeapBitmapInvertedIndexCreator implements InvertedIndexCreator {
    private static final Logger logger = LoggerFactory.getLogger(HeapBitmapInvertedIndexCreator.class);

    private final File invertedIndexFile;
    private final FieldSpec spec;
    private final MutableRoaringBitmap[] invertedIndex;
    private int cardinality;
    long start = 0;

    public HeapBitmapInvertedIndexCreator(File invertedIndexFile, int cardinality, FieldSpec spec) {
        this.spec = spec;
        this.invertedIndexFile = invertedIndexFile;
        this.cardinality = cardinality;
        invertedIndex = new MutableRoaringBitmap[cardinality];
        for (int i = 0; i < invertedIndex.length; ++i) {
            invertedIndex[i] = new MutableRoaringBitmap();
        }
        start = System.currentTimeMillis();
    }

    @Override
    public void add(int docId, int dictionaryId) {
        invertedIndex[dictionaryId].add(docId);
    }

    @Override
    public long totalTimeTakeSoFar() {
        return (System.currentTimeMillis() - start);
    }

    @Override
    public void seal() throws IOException {
        final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));

        // The first 4 bytes stores the cardinality.
        out.writeInt(cardinality);

        // First, write out offsets of bitmaps. The information can be used to access a certain bitmap directly.
        // Totally (invertedIndex.length+1) offsets will be written out; the last offset is used to calculate the length of
        // the last bitmap, which might be needed when accessing bitmaps randomly.
        // If a bitmap's offset is k, then k bytes need to be skipped to reach the bitmap.
        int offset = 4 * (invertedIndex.length + 1); // The first bitmap's offset
        out.writeInt(offset);
        for (final MutableRoaringBitmap element : invertedIndex) { // the other bitmap's offset
            offset += element.serializedSizeInBytes();
            out.writeInt(offset);
        }

        // write out bitmaps one by one
        for (final MutableRoaringBitmap element : invertedIndex) {
            element.serialize(out);
        }
        out.close();
        logger.debug("persisted bitmap inverted index for column : " + spec.getName() + " in " + invertedIndexFile.getAbsolutePath());
    }

    @Override
    public void add(int docId, int[] dictionaryIds) {
        add(docId, dictionaryIds, dictionaryIds.length);
    }

    @Override
    public void add(int docId, int[] dictionaryIds, int length) {
        if (spec.isSingleValueField()) {
            throw new RuntimeException("Method not applicable to single value fields");
        }
        Arrays.sort(dictionaryIds, 0, length);
        indexMultiValue(docId, dictionaryIds, length);
    }

    private void indexSingleValue(int entry, int docId) {
        if (entry == -1) {
            return;
        }
        invertedIndex[entry].add(docId);
    }

    private void indexMultiValue(int docId, int[] entries, int length) {
        for (int i = 0; i < length; i++) {
            final int entry = entries[i];
            if (entry != -1) {
                invertedIndex[entry].add(docId);
            }
        }
    }
}
