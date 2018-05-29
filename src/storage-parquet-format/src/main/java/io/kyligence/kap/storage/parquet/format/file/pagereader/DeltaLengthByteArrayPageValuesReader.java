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

import org.apache.parquet.Log;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.io.api.Binary;

import io.kyligence.kap.common.util.ExpandableBytesVector;

public class DeltaLengthByteArrayPageValuesReader extends PageValuesReader {
    private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesReader.class);
    private ValuesReader lengthReader = new DeltaBinaryPackingValuesReader();
    private ByteBuffer in;
    private int offset;
    private int valueCount;

    public DeltaLengthByteArrayPageValuesReader() {
    }

    public void initFromPage(int valueCount, ByteBuffer in, int offset) throws IOException {
        if (Log.DEBUG) {
            LOG.debug("init from page at offset " + offset + " for length " + (in.limit() - offset));
        }

        this.lengthReader.initFromPage(valueCount, in, offset);
        offset = this.lengthReader.getNextOffset();
        this.in = in;
        this.offset = offset;
        this.valueCount = valueCount;
    }

    public Binary readBytes() {
        int length = this.lengthReader.readInteger();
        int start = this.offset;
        this.offset = start + length;
        return Binary.fromConstantByteBuffer(this.in, start, length);
    }

    public int readBytes(ExpandableBytesVector buffer) {
        int length = this.lengthReader.readInteger();
        int start = this.offset;
        this.offset = start + length;
        if (buffer.remain() < length) {
            buffer.expand(length);
        }
        int position = this.in.position();
        this.in.position(start);
        this.in.get(buffer.getData(), buffer.getTotalLength(), length);
        this.in.position(position);
        buffer.growLength(length);
        return length;
    }

    @Override
    public void readPage(ExpandableBytesVector buffer) {
        int[] offsets = new int[valueCount + 1];
        offsets[0] = this.offset;
        for (int i = 1; i < valueCount + 1; i++) {
            this.offset += lengthReader.readInteger();
            offsets[i] = this.offset;
        }
        buffer.reset(in.array(), valueCount, offsets);
    }

    public void skip() {
        int length = this.lengthReader.readInteger();
        this.offset += length;
    }
}
