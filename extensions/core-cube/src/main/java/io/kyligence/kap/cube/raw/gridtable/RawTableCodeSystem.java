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

package io.kyligence.kap.cube.raw.gridtable;

import java.nio.ByteBuffer;
import java.util.List;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.raw.RawTableColumnDesc;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.gridtable.DefaultGTComparator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.datatype.OrderedBytesSerializer;

public class RawTableCodeSystem implements IGTCodeSystem , IKeep {

    protected static final Logger logger = LoggerFactory.getLogger(RawTableCodeSystem.class);

    GTInfo info;

    List<Pair<String, Integer>> encodings;
    DataTypeSerializer[] serializers;
    IGTComparator comparator;

    /**
     * 
     * @param encodings is a array whose length equals column number of current GTinfo
     *                  var encoding for DataTypeSerializer
     *                  orderedbytes encoding for OrderedBytesSerializer
     *                  and other encodings from cube (except for dict)
     */
    public RawTableCodeSystem(List<Pair<String, Integer>> encodings) {
        this.comparator = new DefaultGTComparator();
        this.encodings = encodings;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];

        StringBuilder logBuffer = new StringBuilder();
        for (int i = 0; i < serializers.length; i++) {
            String encoding;
            Integer encodingVersion;
            if (encodings == null || encodings.size() == 0) {
                encoding = null;
                encodingVersion = 1;
            } else {
                encoding = encodings.get(i).getFirst();
                encodingVersion = encodings.get(i).getSecond();
            }

            if (encoding == null || RawTableColumnDesc.RAWTABLE_ENCODING_VAR.equals(encoding)) {
                logBuffer.append("(" + i + ") type: " + info.getColumnType(i) + " enc: var, ");
                serializers[i] = DataTypeSerializer.create(info.getColumnType(i));
            } else if (RawTableColumnDesc.RAWTABLE_ENCODING_ORDEREDBYTES.equals(encoding)) {
                logBuffer.append("(" + i + ") type: " + info.getColumnType(i) + " enc: orderedbytes, ");
                serializers[i] = OrderedBytesSerializer.createOrdered(info.getColumnType(i));
            } else {
                logBuffer.append("(" + i + ") type: " + info.getColumnType(i) + " enc: " + encoding + ", ");

                Preconditions.checkState(StringUtils.isNotEmpty(encoding));
                Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                String encodingName = (String) encodingConf[0];
                String[] encodingArgs = (String[]) encodingConf[1];

                if (DictionaryDimEnc.ENCODING_NAME.equals(encodingName)) {
                    throw new IllegalArgumentException("Dict encoding is not allowed for raw tables");
                }

                encodingArgs = DateDimEnc.replaceEncodingArgs(encoding, encodingArgs, encodingName, info.getColumnType(i));

                DimensionEncoding dimensionEncoding = DimensionEncodingFactory.create(encodingName, encodingArgs, encodingVersion);
                serializers[i] = dimensionEncoding.asDataTypeSerializer();
            }
        }

        logger.info("Summary from RawTableCodeSystem init: " + logBuffer.toString());
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

    @SuppressWarnings("unused") //used by reflection
    public static final BytesSerializer<IGTCodeSystem> serializer = new BytesSerializer<IGTCodeSystem>() {
        @Override
        public void serialize(IGTCodeSystem ivalue, ByteBuffer out) {
            RawTableCodeSystem value = (RawTableCodeSystem) ivalue;
            if (value.encodings == null) {
                BytesUtil.writeVInt(-1, out);
            } else {
                BytesUtil.writeVInt(value.encodings.size(), out);
                for (int i = 0; i < value.encodings.size(); i++) {
                    BytesUtil.writeAsciiString(value.encodings.get(i).getFirst(), out);
                    BytesUtil.writeVInt(value.encodings.get(i).getSecond(), out);
                }
            }
        }

        @Override
        public RawTableCodeSystem deserialize(ByteBuffer in) {
            int length = BytesUtil.readVInt(in);
            List<Pair<String, Integer>> desEncodings = null;
            if (length != -1) {
                desEncodings = Lists.newArrayListWithExpectedSize(length);
                for (int i = 0; i < length; i++) {
                    String encodingStr = BytesUtil.readAsciiString(in);
                    int encodingVersion = BytesUtil.readVInt(in);
                    desEncodings.add(Pair.newPair(encodingStr, encodingVersion));
                }
            }
            return new RawTableCodeSystem(desEncodings);
        }
    };

}
