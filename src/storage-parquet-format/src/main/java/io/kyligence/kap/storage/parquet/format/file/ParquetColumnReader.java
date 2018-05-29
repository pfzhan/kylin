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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import io.kyligence.kap.common.util.ExpandableBytesVector;
import io.kyligence.kap.storage.parquet.format.file.pagereader.PageValuesReader;

public class ParquetColumnReader {
    public final int INIT_BUFFER_SIZE = 10000;
    private ParquetRawReader reader;
    public int column;
    private int curPage = -1;
    private PeekableIntIterator iter = null;
    private ExpandableBytesVector buffer = null;

    public ParquetColumnReader(ParquetRawReader reader, int column, ImmutableRoaringBitmap pageBitset) {
        this.reader = reader;
        this.column = column;
        if (pageBitset != null) {
            iter = pageBitset.getIntIterator();
        }
    }

    public int getPageIndex() {
        return curPage;
    }

    /**
     * Return next page's buffer, it there's no next page, return null
     */
    public ExpandableBytesVector readNextPage() throws IOException {
        reader.metrics.pageReadOverallStart();

        // read one page
        PageValuesReader pageReader = null;
        {
            if (iter != null) { // has page bitmap
                if (iter.hasNext()) {
                    curPage = iter.next();
                    pageReader = reader.getPageValuesReader(curPage, column);
                }
            } else { // no page bitmap, read all pages
                pageReader = reader.getPageValuesReader(++curPage, column);
            }
            if (pageReader == null)
                return null; // don't call metrics.pageReadOverallEnd(), no page is read
        }
        
        // decode the page into BytesVector
        reader.metrics.pageReadDecodeStart();
        {
            if (buffer == null)
                buffer = new ExpandableBytesVector(INIT_BUFFER_SIZE);
            buffer.setTotalLength(0);
            buffer.setRowCount(0);

            pageReader.readPage(buffer);
        }
        reader.metrics.pageReadDecodeEnd(buffer.getTotalLength());
        
        reader.metrics.pageReadOverallEnd(buffer.getRowCount());
        return buffer;
    }

    /**
     * Get next page values reader
     * @return values reader, if returns null, there's no page left
     */
    public GeneralValuesReader getNextValuesReader() throws IOException {
        if (iter != null) {
            if (iter.hasNext()) {
                curPage = iter.next();
                return reader.getValuesReader(curPage, column);
            }
            return null;
        }
        return reader.getValuesReader(++curPage, column);
    }
}
