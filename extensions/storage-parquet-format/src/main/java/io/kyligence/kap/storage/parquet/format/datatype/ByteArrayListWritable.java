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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ByteArrayListWritable implements WritableComparable<ByteArrayListWritable> {

    private List<byte[]> value;

    public ByteArrayListWritable() {
        value = new ArrayList<>();
    }

    public ByteArrayListWritable(List<byte[]> value) {
        this.value = value;
    }

    public ByteArrayListWritable(byte[]... value) {
        int len = value.length;
        this.value = new ArrayList<>(len);
        for (int i = 0; i < len; ++i) {
            this.value.add(value[i]);
        }
    }

    public List<byte[]> get() {
        return value;
    }

    public void add(byte[] val) {
        value.add(val);
    }

    @Override
    public int compareTo(ByteArrayListWritable that) {
        Iterator<byte[]> thisIter = this.get().iterator();
        Iterator<byte[]> thatIter = that.get().iterator();
        while (true) {
            if (thisIter.hasNext()) {
                if (thatIter.hasNext()) {
                    byte[] thisE = thisIter.next();
                    byte[] thatE = thatIter.next();
                    int comp = WritableComparator.compareBytes(thisE, 0, thisE.length, thatE, 0, thatE.length);
                    if (comp == 0) {
                        continue;
                    }
                    return comp;
                } else {
                    return 1;
                }
            } else {
                if (thatIter.hasNext()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
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

    @Override
    public int hashCode() {
        int result = 17;
        if (value != null) {
            for (byte[] v : value) {
                result = 31 * result + (v == null || v.length == 0 ? 0 : v[0]);
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ByteArrayListWritable)) {
            return false;
        }

        ByteArrayListWritable castObj = (ByteArrayListWritable) obj;
        return this.compareTo(castObj) == 0;
    }

    public static class Comparator extends WritableComparator {
        /** constructor */
        public Comparator() {
            super(ByteArrayListWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof ByteArrayListWritable && b instanceof ByteArrayListWritable) {
                return ((ByteArrayListWritable) a).compareTo((ByteArrayListWritable) b);
            }
            return 0;
        }
    }

    static { // register this comparator
        WritableComparator.define(ByteArrayListWritable.class, new ByteArrayListWritable.Comparator());
    }
}
