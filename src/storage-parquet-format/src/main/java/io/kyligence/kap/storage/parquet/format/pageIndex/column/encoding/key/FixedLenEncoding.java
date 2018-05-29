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

package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;

import com.google.common.base.Preconditions;

public class FixedLenEncoding implements IKeyEncoding<ByteArray> {
    private int length;

    public FixedLenEncoding(int length) {
        this.length = length;
    }

    @Override
    public ByteArray encode(ByteArray value) {
        Preconditions.checkState(value.length() == length);
        return value;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public void serialize(ByteArray value, DataOutputStream outputStream) throws IOException {
        Preconditions.checkState(value.length() == length);
        outputStream.write(value.array(), value.offset(), value.length());
    }

    @Override
    public ByteArray deserialize(DataInputStream inputStream) throws IOException {
        ByteArray value = ByteArray.allocate(length);
        inputStream.readFully(value.array());
        return value;
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.FIXED_LEN.getIdentifier();
    }
}
