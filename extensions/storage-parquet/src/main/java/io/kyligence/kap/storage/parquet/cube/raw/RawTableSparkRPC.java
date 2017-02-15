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

package io.kyligence.kap.storage.parquet.cube.raw;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.IStorageVisitResponseStreamer;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClientParams;

public class RawTableSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(RawTableSparkRPC.class);

    protected RawTableSegment rawTableSegment;
    protected Cuboid cuboid;
    protected GTInfo info;
    private SparkDriverClient client;

    public RawTableSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        this.rawTableSegment = (RawTableSegment) segment;
        this.cuboid = cuboid;
        this.info = info;
        this.init();
    }

    protected void init() {
        try {
            client = new SparkDriverClient(KapConfig.getInstanceFromEnv());
        } catch (Exception e) {
            logger.error("error is " + e.getLocalizedMessage());
            throw e;
        }
    }

    protected List<Integer> getRequiredParquetColumns(GTScanRequest request) {
        List<Integer> ret = Lists.newArrayList();
        ImmutableBitSet immutableBitSet = request.getColumns();
        for (int i = 0; i < immutableBitSet.trueBitCount(); i++) {
            ret.add(immutableBitSet.trueBitAt(i));
        }
        return ret;
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());

        final long startTime = System.currentTimeMillis();
        SparkDriverClientParams sparkDriverClientParams = new SparkDriverClientParams(KylinConfig.getInstanceFromEnv().getConfigAsString(), //
                RealizationType.INVERTED_INDEX.toString(), rawTableSegment.getRawTableInstance().getUuid(), rawTableSegment.getUuid(), "RawTable", // 
                scanRequest.getInfo().getMaxLength(), getRequiredParquetColumns(scanRequest), KapConfig.getInstanceFromEnv().isUsingInvertedIndex(), //
                QueryContext.current().getQueryId(), KylinConfig.getInstanceFromEnv().getQueryCoprocessorSpillEnabled(), KylinConfig.getInstanceFromEnv().getPartitionMaxScanBytes());
        logger.info("Filter: {}", scanRequest.getFilterPushDown());

        final IStorageVisitResponseStreamer storageVisitResponseStreamer = client.submit(scanRequest, sparkDriverClientParams);
        return new StorageResponseGTScatter(info, storageVisitResponseStreamer, scanRequest.getColumns(), 0, scanRequest.getStoragePushDownLimit());
    }
}
