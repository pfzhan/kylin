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

package io.kyligence.kap.storage.parquet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;

public class NDataflowSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(NDataflowSparkRPC.class);

    protected NDataSegment dataSegment;
    protected NCuboidLayout cuboid;
    protected GTInfo info;
    protected StorageContext context;

    //private SparkDriverClient client;

    public NDataflowSparkRPC(ISegment segment, NCuboidLayout cuboid, GTInfo info, StorageContext context) {
        this.dataSegment = (NDataSegment) segment;
        this.cuboid = cuboid;
        this.info = info;
        this.context = context;

        init();
    }

    protected void init() {
        try {
            //client = new SparkDriverClient(KapConfig.getInstanceFromEnv());
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

        return null;
        //        //TODO: IMPORTANT: scanRequest's timeout is set value here
        //        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        //
        //        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());
        //        logger.info("Filter: {}", scanRequest.getFilterPushDown());
        //
        //        String scanReqId = Integer.toHexString(System.identityHashCode(scanRequest));
        //
        //        SparkJobProtos.SparkJobRequestPayload payload = SparkJobProtos.SparkJobRequestPayload.newBuilder()
        //                .setGtScanRequest(ByteString.copyFrom(scanRequest.toByteArray())).setGtScanRequestId(scanReqId)
        //                .setKylinProperties(KylinConfig.getInstanceFromEnv().exportToString())
        //                .setRealizationId(dataSegment.getDataflow().getUuid())
        //                .setSegmentId(Integer.toString(dataSegment.getId())).setDataFolderName(String.valueOf(cuboid.getId()))
        //                .setMaxRecordLength(scanRequest.getInfo().getMaxLength())
        //                .addAllParquetColumns(getRequiredParquetColumns(scanRequest))
        //                .setUseII(KapConfig.getInstanceFromEnv().isUsingInvertedIndex())
        //                .setRealizationType(dataSegment.getDataflow().getType()).setQueryId(QueryContext.current().getQueryId())
        //                .setSpillEnabled(dataSegment.getConfig().getQueryCoprocessorSpillEnabled())
        //                .setMaxScanBytes(dataSegment.getConfig().getPartitionMaxScanBytes())
        //                .setStartTime(scanRequest.getStartTime()).setStorageType(cuboid.getStorageType()).build();
        //
        //        if (BackdoorToggles.getDumpedPartitionDir() != null) {
        //            logger.info("debugging: use previously dumped partition from {} instead of real requesting from storage",
        //                    BackdoorToggles.getDumpedPartitionDir());
        //            return new StorageResponseGTScatter(scanRequest,
        //                    new DummyPartitionStreamer(new PartitionIteratorFromDir(BackdoorToggles.getDumpedPartitionDir())),
        //                    context);
        //        }
        //
        //        logger.info("The scan {} for segment {} is ready to be submitted to spark client", scanReqId, dataSegment);
        //        final IStorageVisitResponseStreamer storageVisitResponseStreamer = client.submit(scanRequest, payload,
        //                dataSegment.getConfig().getQueryMaxScanBytes());
        //        return new StorageResponseGTScatter(scanRequest, storageVisitResponseStreamer, context);

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
