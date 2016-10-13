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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueReader;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueWriter;

public class GTColumnForwardIndex implements IColumnForwardIndex {
    protected static final Logger logger = LoggerFactory.getLogger(GTColumnForwardIndex.class);

    private final String idxFilename;
    private final int fixedBitsNum;
    private final String colName;
    private boolean useMmap;

    public GTColumnForwardIndex(String colName, int maxValue, String idxFilename) {
        this.colName = colName;
        this.idxFilename = idxFilename;
        this.fixedBitsNum = Integer.SIZE - Integer.numberOfLeadingZeros(maxValue);
    }

    @Override
    public Builder rebuild() {
        try {
            return new GTColumnForwardIndexBuilder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Reader getReader() {
        try {
            return new GTColumnForwardIndexReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setUseMmap(boolean b) {
        this.useMmap = b;
    }

    public void deleteIndexFile() {
        new File(idxFilename).delete();
    }

    private class GTColumnForwardIndexBuilder implements IColumnForwardIndex.Builder {
        final FixedBitSingleValueWriter writer;
        int rowCounter = 0;

        public GTColumnForwardIndexBuilder() throws Exception {
            this.writer = new FixedBitSingleValueWriter(new File(idxFilename), fixedBitsNum);
        }

        @Override
        public void putNextRow(int v) {
            writer.setInt(rowCounter++, v);
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private class GTColumnForwardIndexReader implements IColumnForwardIndex.Reader {
        FixedBitSingleValueReader reader;

        public GTColumnForwardIndexReader() throws IOException {
            if (useMmap)
                this.reader = FixedBitSingleValueReader.forMmap(new File(idxFilename), fixedBitsNum);
            else
                this.reader = FixedBitSingleValueReader.forHeap(new File(idxFilename), fixedBitsNum);
        }

        @Override
        public int get(int row) {
            return reader.getInt(row);
        }

        @Override
        public int getNumberOfRows() {
            return reader.getNumberOfRows();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
