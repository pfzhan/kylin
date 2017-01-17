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

package io.kyligence.kap.measure.indexedraw;

import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class IndexedRawMeasureType extends MeasureType<ByteArray> {
    private static final long serialVersionUID = 1L;
    
    private final DataType dataType;

    public IndexedRawMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public MeasureIngester<ByteArray> newIngester() {
        return new MeasureIngester<ByteArray>() {
            private static final long serialVersionUID = 1L;

            //encode measure value to dictionary
            @Override
            public ByteArray valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 1)
                    throw new IllegalArgumentException();

                //source input column value
                String literal = values[0];
                // encode literal using dictionary
                TblColRef literalCol = getRawColumn(measureDesc.getFunction());
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                if (dictionary != null) {
                    ByteArray key = null;
                    int keyEncodedValue = dictionary.getIdFromValue(literal);

                    key = new ByteArray(dictionary.getSizeOfId());
                    BytesUtil.writeUnsigned(keyEncodedValue, key.array(), key.offset(), dictionary.getSizeOfId());
                    return key;
                }
                return new ByteArray(literal.getBytes());
            }

            //merge measure dictionary
            @Override
            public ByteArray reEncodeDictionary(ByteArray value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TblColRef colRef = getRawColumn(measureDesc.getFunction());
                Dictionary<String> sourceDict = oldDicts.get(colRef);
                Dictionary<String> mergedDict = newDicts.get(colRef);

                byte[] newIdBuf = new byte[mergedDict.getSizeOfId()];

                int oldId = BytesUtil.readUnsigned(value.array(), value.offset(), value.length());
                int newId;
                String v = sourceDict.getValueFromId(oldId);
                if (v == null) {
                    newId = mergedDict.nullId();
                } else {
                    newId = mergedDict.getIdFromValue(v);
                }
                BytesUtil.writeUnsigned(newId, newIdBuf, 0, mergedDict.getSizeOfId());
                return new ByteArray(newIdBuf);
            }
        };
    }

    @SuppressWarnings("serial")
    @Override
    public MeasureAggregator<ByteArray> newAggregator() {
        return new MeasureAggregator<ByteArray>() {
            private ByteArray byteArray = null;

            @Override
            public void reset() {
                byteArray = null;
            }

            @Override
            public void aggregate(ByteArray value) {
                byteArray = value;
            }

            @Override
            public ByteArray aggregate(ByteArray value1, ByteArray value2) {
                if (value1 != null)
                    return value1;
                return value2;
            }

            @Override
            public ByteArray getState() {
                return byteArray;
            }

            @Override
            public int getMemBytesEstimate() {
                return dataType.getPrecision();
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    private TblColRef getRawColumn(FunctionDesc functionDesc) {
        return functionDesc.getParameter().getColRefs().get(0);
    }
}
