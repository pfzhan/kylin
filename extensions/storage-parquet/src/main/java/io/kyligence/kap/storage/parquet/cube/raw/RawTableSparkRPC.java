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
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.IPartitionStreamer;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.cube.spark.refactor.SparkSubmitter;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.protocol.shaded.com.google.protobuf.ByteString;

public class RawTableSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(RawTableSparkRPC.class);

    protected RawTableSegment rawTableSegment;
    protected Cuboid cuboid;
    protected GTInfo info;
    protected StorageContext context;
    private SparkSubmitter client;

    public RawTableSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info, StorageContext context) {
        this.rawTableSegment = (RawTableSegment) segment;
        this.cuboid = cuboid;
        this.info = info;
        this.context = context;
        this.init();
    }

    protected void init() {
        try {
            client = new SparkSubmitter();
        } catch (Exception e) {
            logger.error("error is " + e.getLocalizedMessage());
            throw e;
        }
    }

    protected List<Integer> getRequiredParquetColumns(GTScanRequest request) {
        List<Integer> columnFamilies = Lists.newArrayList();

        for (int i = 0; i < request.getSelectedColBlocks().trueBitCount(); i++) {
            columnFamilies.add(request.getSelectedColBlocks().trueBitAt(i));
        }

        return columnFamilies;
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());
        logger.info("Filter: {}", scanRequest.getFilterPushDown());

        String scanReqId = Integer.toHexString(System.identityHashCode(scanRequest));

        SparkJobProtos.SparkJobRequestPayload.Builder builder = SparkJobProtos.SparkJobRequestPayload.newBuilder()
                .setGtScanRequest(ByteString.copyFrom(scanRequest.toByteArray())).//
                setGtScanRequestId(scanReqId).setKylinProperties(KylinConfig.getInstanceFromEnv().exportAllToString())
                .setRealizationId(rawTableSegment.getRawTableInstance().getUuid()).//
                setSegmentId(rawTableSegment.getUuid()).setDataFolderName(String.valueOf("RawTable")).//
                setMaxRecordLength(scanRequest.getInfo().getMaxLength())
                .addAllParquetColumns(getRequiredParquetColumns(scanRequest)).//
                setUseII(KapConfig.getInstanceFromEnv().isUsingInvertedIndex())
                .setRealizationType(rawTableSegment.getRawTableInstance().getType()).//
                setQueryId(QueryContext.current().getQueryId())
                .setSpillEnabled(rawTableSegment.getConfig().getQueryCoprocessorSpillEnabled()).//
                setMaxScanBytes(rawTableSegment.getConfig().getPartitionMaxScanBytes())
                .setStartTime(scanRequest.getStartTime());

        SparkJobProtos.SparkJobRequestPayload payload = builder.setStorageType(-1).build();

        logger.info("The scan {} for segment {} is ready to be submitted to spark client", scanReqId, rawTableSegment);

        final IPartitionStreamer storageVisitResponseStreamer = client.submitParquetTask(scanRequest, payload,
                rawTableSegment.getConfig().getQueryMaxScanBytes());
        return new StorageResponseGTScatter(scanRequest, storageVisitResponseStreamer, context);
    }
}
