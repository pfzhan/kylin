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

package io.kyligence.kap.storage.parquet.format.file.pagereader;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.kyligence.kap.common.util.ExpandableBytesVector;
import org.apache.kylin.common.util.ByteArray;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;

public class DeltaByteArrayPageReader extends PageValuesReader implements RequiresPreviousReader {
    private ValuesReader prefixLengthReader = new DeltaBinaryPackingValuesReader();
    private PageValuesReader suffixReader = new DeltaLengthByteArrayPageValuesReader();
    private Binary previous = Binary.fromConstantByteArray(new byte[0]);
    private ByteArray previous2 = new ByteArray();
    private int rowNum;

    public DeltaByteArrayPageReader() {
    }

    public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
        this.rowNum = valueCount;
        this.prefixLengthReader.initFromPage(valueCount, page, offset);
        int next = this.prefixLengthReader.getNextOffset();
        this.suffixReader.initFromPage(valueCount, page, next);
    }

    public void skip() {
        this.readBytes();
    }

    public Binary readBytes() {
        int prefixLength = this.prefixLengthReader.readInteger();
        Binary suffix = this.suffixReader.readBytes();
        int length = prefixLength + suffix.length();
        if (prefixLength != 0) {
            byte[] out = new byte[length];
            System.arraycopy(this.previous.getBytesUnsafe(), 0, out, 0, prefixLength);
            System.arraycopy(suffix.getBytesUnsafe(), 0, out, prefixLength, suffix.length());
            this.previous = Binary.fromConstantByteArray(out);
        } else {
            this.previous = suffix;
        }

        return this.previous;
    }

    public int readBytes(ExpandableBytesVector buffer) {
        int prefixLength = this.prefixLengthReader.readInteger();
        int position = buffer.getTotalLength();
        if (prefixLength != 0) {
            if (buffer.remain() < prefixLength) {
                buffer.expand(prefixLength);
            }
            System.arraycopy(this.previous2.array(), this.previous2.offset(), buffer.getData(), buffer.getTotalLength(), prefixLength);
            buffer.growLength(prefixLength);
        }
        int suffixLength = this.suffixReader.readBytes(buffer);
        int length = prefixLength + suffixLength;
        this.previous2.reset(buffer.getData(), position, length);
        return length;
    }

    public void readPage(ExpandableBytesVector buffer) {
        buffer.setRowCount(rowNum);
        int prevOffset = 0;
        int i = 0;
        for (; i < rowNum; i++) {
            buffer.setOffset(i, prevOffset);
            prevOffset += readBytes(buffer);
        }
        buffer.setOffset(i, prevOffset);
    }

    public void setPreviousReader(ValuesReader reader) {
        if (reader != null) {
            this.previous = ((DeltaByteArrayPageReader) reader).previous;
        }
    }
}
