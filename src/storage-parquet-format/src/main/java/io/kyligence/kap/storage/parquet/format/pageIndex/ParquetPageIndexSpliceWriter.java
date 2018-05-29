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

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

public class ParquetPageIndexSpliceWriter implements Closeable {
    private boolean divStarted;
    private long curCuboid;
    private DataOutputStream outputStream;
    private ParquetPageIndexWriter writer;
    private ParquetPageIndexSpliceMeta cuboidMeta;
    private long size = 0;
    private int pageStart = -1;
    private int pageEnd = -1;

    public ParquetPageIndexSpliceWriter(DataOutputStream outputStream) {
        this.outputStream = outputStream;
        cuboidMeta = new ParquetPageIndexSpliceMeta();
    }

    public boolean isDivStarted() {
        return divStarted;
    }

    public void startDiv(long cuboid, String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex) throws IOException {
        divStarted = true;
        curCuboid = cuboid;
        writer = new ParquetPageIndexWriter(columnNames, columnLength, cardinality, onlyEQIndex, outputStream);
    }

    public void endDiv() throws IOException {
        if (!divStarted) {
            return;
        }
        divStarted = false;
        writer.closeWithoutStream();
        // the range is [pageStart, pageEnd + 1) == [pageStart, pageEnd]
        cuboidMeta.put(curCuboid, size, pageStart, pageEnd + 1);
        size += writer.getCurOffset();
        pageStart = -1;
        pageEnd = -1;
    }

    public void write(byte[] rowKey, int pageId) {
        if (divStarted && null != writer) {
            writer.write(rowKey, pageId);
            if (pageStart < 0) {
                pageStart = pageId;
            }
            pageEnd = pageId;
        }
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        if (divStarted && null != writer) {
            writer.write(rowKey, startOffset, pageId);
            if (pageStart < 0) {
                pageStart = pageId;
            }
            pageEnd = pageId;
        }
    }

    public void write(List<byte[]> rowKeys, int pageId) {
        if (divStarted && null != writer) {
            writer.write(rowKeys, pageId);
            if (pageStart < 0) {
                pageStart = pageId;
            }
            pageEnd = pageId;
        }
    }

    public void spill() {
        if (divStarted && null != writer) {
            writer.spill();
        }
    }

    @Override
    public void close() throws IOException {
        closeWithoutStream().close();
    }

    public ObjectOutputStream closeWithoutStream() throws IOException {
        if (divStarted) {
            endDiv();
        }

        ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        oos.writeObject(cuboidMeta);
        oos.flush();
        outputStream.writeLong(size);
        outputStream.flush();

        return oos;
    }
}
