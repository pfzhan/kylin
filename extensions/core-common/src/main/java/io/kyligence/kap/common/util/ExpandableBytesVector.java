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

package io.kyligence.kap.common.util;

public class ExpandableBytesVector {
    private int length;
    private byte[] data;
    private int[] offset = new int[0];
    private int rowCount = 0;

    public ExpandableBytesVector(int capacity) {
        length = 0;
        data = new byte[capacity];
    }

    public ExpandableBytesVector(byte[] data, int length) {
        this.length = length;
        this.data = data;
    }

    public static ExpandableBytesVector wrap(byte[] data) {
        return new ExpandableBytesVector(data, data.length);
    }

    public void expand() {
        expand(0);
    }

    public void expand(int need) {
        int targetLen = Math.max(need - remain() + data.length, data.length * 2);
        byte[] tmp = new byte[targetLen];
        System.arraycopy(data, 0, tmp, 0, length);
        data = tmp;
    }

    // get length of total buffer
    public int getTotalLength() {
        return length;
    }

    public void setTotalLength(int len) {
        length = len;
    }

    public void growLength(int delta) {
        length += delta;
    }

    public int getCapacity() {
        return data.length;
    }

    public int remain() {
        return data.length - length;
    }

    public byte[] getData() {
        return data;
    }

    public int getOffset(int i) {
        return offset[i];
    }

    public void setOffset(int i, int off) {
        offset[i] = off;
    }

    // return length of a single record
    public int getLength(int i) {
        return offset[i + 1] - offset[i];
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
        if (rowCount > offset.length) {
            offset = new int[rowCount + 1];
        }
    }
    
    public void reset(byte[] data, int valueCount, int[] offsets) {
        this.data = data;
        this.rowCount = valueCount;
        this.offset = offsets;
        this.length = offsets[offsets.length - 1];
        
        if (rowCount != offset.length - 1)
            throw new IllegalStateException();
    }
}
