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

package io.kyligence.kap.storage.parquet.format.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ByteArrayListWritable implements WritableComparable<ByteArrayListWritable> {

    private List<byte[]> value;

    public ByteArrayListWritable(List<byte[]> value) {
        this.value = value;
    }

    public ByteArrayListWritable(byte[][] value) {
        int len = value.length;
        this.value = new ArrayList<>(len);
        for (int i = 0; i < len; ++i) {
            this.value.add(value[i]);
        }
    }

    public List<byte[]> get() {
        return value;
    }

    // Because the order of two dimension array is not that important,
    // we only compare the first byte[]
    @Override
    public int compareTo(ByteArrayListWritable that) {
        byte[] first = this.value.get(0);
        byte[] thatFirst = that.get().get(0);
        return WritableComparator.compareBytes(first, 0, first.length, thatFirst, 0, thatFirst.length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value.size());
        for (byte[] b : value) {
            out.writeInt(b.length);
        }

        for (byte[] b : value) {
            out.write(b);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        value = new ArrayList<>(len);
        for (int i = 0; i < len; ++i) {
            value.add(new byte[in.readInt()]);
        }

        for (int i = 0; i < len; ++i) {
            in.readFully(value.get(i));
        }
    }
}
