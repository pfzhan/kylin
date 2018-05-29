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

package io.kyligence.kap.storage.parquet.format.filter;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.TupleFilter;

public class BinaryConstantFilter implements BinaryFilter{
    private boolean value;

    public BinaryConstantFilter(boolean value) {
        this.value = value;
    }

    public BinaryConstantFilter() {}

    @Override
    public boolean isMatch(ByteArray value) {
        return this.value;
    }

    @Override
    public TupleFilter.FilterOperatorEnum getOperator() {
        return TupleFilter.FilterOperatorEnum.CONSTANT;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        BytesUtil.writeUTFString(TupleFilter.FilterOperatorEnum.CONSTANT.name(), buffer);
        BytesUtil.writeBooleanArray(new boolean[] {value}, buffer);
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        value = BytesUtil.readBooleanArray(buffer)[0];
    }
}
