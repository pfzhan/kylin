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

package org.apache.spark.sql.execution.datasources.sparder;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;

import com.google.common.collect.Lists;

public class TestGtscanRequest {
    public static final BytesSerializer<GTScanRequest> serializer = new BytesSerializer<GTScanRequest>() {
        @Override
        public void serialize(GTScanRequest value, ByteBuffer out) {
        }

        @Override
        public GTScanRequest deserialize(ByteBuffer in) {
            final int serialLevel = KylinConfig.getInstanceFromEnv().getGTScanRequestSerializationLevel();

            GTInfo sInfo = GTInfo.serializer.deserialize(in);

            List<GTScanRange> sRanges = Lists.newArrayList();
            int sRangesCount = BytesUtil.readVInt(in);
            for (int rangeIdx = 0; rangeIdx < sRangesCount; rangeIdx++) {
                GTRecord sPkStart = deserializeGTRecord(in, sInfo);
                GTRecord sPkEnd = deserializeGTRecord(in, sInfo);
                List<GTRecord> sFuzzyKeys = Lists.newArrayList();
                int sFuzzyKeySize = BytesUtil.readVInt(in);
                for (int i = 0; i < sFuzzyKeySize; i++) {
                    sFuzzyKeys.add(deserializeGTRecord(in, sInfo));
                }
                GTScanRange sRange = new GTScanRange(sPkStart, sPkEnd, sFuzzyKeys);
                sRanges.add(sRange);
            }

            ImmutableBitSet sColumns = ImmutableBitSet.serializer.deserialize(in);
            TupleFilter sGTFilter = GTUtil.deserializeGTFilter(BytesUtil.readByteArray(in), sInfo);

            TupleFilter sGTHavingFilter = null;
            if (serialLevel >= 1) {
                sGTHavingFilter = TupleFilterSerializer.deserialize(BytesUtil.readByteArray(in),
                        StringCodeSystem.INSTANCE);
            }

            ImmutableBitSet sAggGroupBy = ImmutableBitSet.serializer.deserialize(in);
            ImmutableBitSet sAggrMetrics = ImmutableBitSet.serializer.deserialize(in);
            String[] sAggrMetricFuncs = BytesUtil.readAsciiStringArray(in);
            boolean sAllowPreAggr = (BytesUtil.readVInt(in) == 1);
            double sAggrCacheGB = in.getDouble();
            //            StorageLimitLevel storageLimitLevel = StorageLimitLevel.valueOf(BytesUtil.readUTFString(in));
            int storageScanRowNumThreshold = BytesUtil.readVInt(in);
            int storagePushDownLimit = BytesUtil.readVInt(in);
            long startTime = BytesUtil.readVLong(in);
            long timeout = BytesUtil.readVLong(in);
            String storageBehavior = BytesUtil.readUTFString(in);

            return new GTScanRequestBuilder().setInfo(sInfo).setRanges(sRanges).setDimensions(sColumns)
                    .setAggrGroupBy(sAggGroupBy).setAggrMetrics(sAggrMetrics).setAggrMetricsFuncs(sAggrMetricFuncs)
                    .setFilterPushDown(sGTFilter).setHavingFilterPushDown(sGTHavingFilter)
                    .setAllowStorageAggregation(sAllowPreAggr).setAggCacheMemThreshold(sAggrCacheGB)
                    .setStorageScanRowNumThreshold(storageScanRowNumThreshold)
                    .setStoragePushDownLimit(storagePushDownLimit)
                    //                    .setStorageLimitLevel(storageLimitLevel)
                    .setStartTime(startTime).setTimeout(timeout).setStorageBehavior(storageBehavior)
                    .createGTScanRequest();
        }

        private GTRecord deserializeGTRecord(ByteBuffer in, GTInfo sInfo) {
            int colLength = BytesUtil.readVInt(in);
            ByteArray[] sCols = new ByteArray[colLength];
            for (int i = 0; i < colLength; i++) {
                sCols[i] = ByteArray.importData(in);
            }
            return new GTRecord(sInfo, sCols);
        }

    };
}
