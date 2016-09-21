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

package io.kyligence.kap.cube.raw.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.DefaultGTComparator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.datatype.OrderedBytesSerializer;

public class RawTableCodeSystem implements IGTCodeSystem {

    protected static final Logger logger = LoggerFactory.getLogger(RawTableCodeSystem.class);

    GTInfo info;

    DimensionEncoding[] dimensionEncodings;
    DataTypeSerializer[] serializers;
    IGTComparator comparator;

    /**
     * 
     * @param dimensionEncodings is a array whose length equals column number of current GTinfo
     *                           however each array value is nullable. Null means using OrderedBytes
     */
    public RawTableCodeSystem(DimensionEncoding[] dimensionEncodings) {
        this.comparator = new DefaultGTComparator();
        this.dimensionEncodings = dimensionEncodings;
    }

    /**
     * temporal , don't use it!
     */
    public RawTableCodeSystem() {
        this.comparator = new DefaultGTComparator();
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < serializers.length; i++) {
            if (dimensionEncodings != null && dimensionEncodings[i] != null) {
                serializers[i] = this.dimensionEncodings[i].asDataTypeSerializer();
            } else {
                logger.info("info column type {} {} {}", i, info.getColumnType(i), info.getColumnType(i).getName());
                serializers[i] = OrderedBytesSerializer.createOrdered(info.getColumnType(i));
                //serializers[i] = DataTypeSerializer.create(info.getColumnType(i));
            }
        }
    }

    @Override
    public IGTComparator getComparator() {
        return comparator;
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return serializers[col].peekLength(buf);
    }

    @Override
    public int maxCodeLength(int col) {
        return serializers[col].maxLength();
    }

    @Override
    public DimensionEncoding getDimEnc(int col) {
        return null;
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        encodeColumnValue(col, value, 0, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = serializers[col];
        if (serializer instanceof DictionaryDimEnc.DictionarySerializer) {
            //TODO:
            throw new IllegalStateException("Raw table does not support dict now");
        } else {
            if (value instanceof String) {
                // for dimensions; measures are converted by MeasureIngestor before reaching this point
                value = serializer.valueOf((String) value);
            }
            serializer.serialize(value, buf);
        }
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

    @Override
    public MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions) {
        throw new UnsupportedOperationException();
    }

    public DataTypeSerializer getSerializer(int idx) {
        return serializers[idx];
    }

    public int getColumnCount() {
        return serializers.length;
    }
}
