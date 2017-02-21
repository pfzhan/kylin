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

package io.kyligence.kap.storage.parquet.cube;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.gtrecord.DummyPartitionStreamer;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.IStorageVisitResponseStreamer;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import kap.google.protobuf.ByteString;

public class CubeSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(CubeSparkRPC.class);

    protected CubeSegment cubeSegment;
    protected Cuboid cuboid;
    protected GTInfo info;

    private SparkDriverClient client;

    public CubeSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        this.cubeSegment = (CubeSegment) segment;
        this.cuboid = cuboid;
        this.info = info;

        init();
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
        List<Integer> measures = Lists.newArrayList(0);//the row key parquet column
        int numDim = request.getInfo().getPrimaryKey().trueBitCount();
        for (int i = 0; i < request.getAggrMetrics().trueBitCount(); i++) {
            int index = request.getAggrMetrics().trueBitAt(i);
            measures.add(index - numDim + 1);
        }
        return measures;

    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());

        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());
        logger.info("Filter: {}", scanRequest.getFilterPushDown());

        SparkJobProtos.SparkJobRequestPayload payload = SparkJobProtos.SparkJobRequestPayload.newBuilder().setGtScanRequest(ByteString.copyFrom(scanRequest.toByteArray())).//
                setKylinProperties(KylinConfig.getInstanceFromEnv().getConfigAsString()).setRealizationId(cubeSegment.getCubeInstance().getUuid()).//
                setSegmentId(cubeSegment.getUuid()).setDataFolderName(String.valueOf(cuboid.getId())).//
                setMaxRecordLength(scanRequest.getInfo().getMaxLength()).addAllParquetColumns(getRequiredParquetColumns(scanRequest)).//
                setUseII(KapConfig.getInstanceFromEnv().isUsingInvertedIndex()).setRealizationType(RealizationType.CUBE.toString()).//
                setQueryId(QueryContext.current().getQueryId()).setSpillEnabled(cubeSegment.getConfig().getQueryCoprocessorSpillEnabled()).setMaxScanBytes(cubeSegment.getConfig().getPartitionMaxScanBytes()).//
                build();

        if (BackdoorToggles.getDumpedPartitionDir() != null) {
            logger.info("debugging: use previously dumped partition from {} instead of real requesting from storage", BackdoorToggles.getDumpedPartitionDir());
            return new StorageResponseGTScatter(info, new DummyPartitionStreamer(new PartitionIteratorFromDir(BackdoorToggles.getDumpedPartitionDir())), scanRequest.getColumns(), scanRequest.getStoragePushDownLimit());
        }

        logger.info("The scan {} for segment {} is ready to be submitted to spark client", Integer.toHexString(System.identityHashCode(scanRequest)), cubeSegment);
        final IStorageVisitResponseStreamer storageVisitResponseStreamer = client.submit(scanRequest, payload, cubeSegment.getConfig().getQueryMaxScanBytes());
        return new StorageResponseGTScatter(info, storageVisitResponseStreamer, scanRequest.getColumns(), scanRequest.getStoragePushDownLimit());

    }

    //only for debug/profile purpose
    private static class PartitionIteratorFromDir extends UnmodifiableIterator<byte[]> {

        private final UnmodifiableIterator<File> fileUnmodifiableIterator;

        public PartitionIteratorFromDir(String dirStr) {
            File dir = new File(dirStr);

            if (!dir.exists() || !dir.isDirectory() || dir.listFiles().length == 0) {
                throw new IllegalArgumentException("{} is not legal dir for BytesIteratorFromDir");
            }

            File[] files = dir.listFiles();
            fileUnmodifiableIterator = Iterators.forArray(files);
        }

        @Override
        public boolean hasNext() {
            return fileUnmodifiableIterator.hasNext();
        }

        @Override
        public byte[] next() {
            File next = fileUnmodifiableIterator.next();
            try (InputStream in = new FileInputStream(next)) {
                return IOUtils.toByteArray(in);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
