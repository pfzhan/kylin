/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.source.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

//used by reflection
public class KafkaSource implements ISource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMRInput.class) {
            return (I) new KafkaMRInput();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange segmentRange) {

        SegmentRange.KafkaOffsetPartitionedSegmentRange ksr = null;
        Map<Integer, Long> sourcePartitionOffsetStart = null;
        Map<Integer, Long> sourcePartitionOffsetEnd = null;

        if (segmentRange != null) {
            Preconditions.checkState(segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange);
            ksr = (SegmentRange.KafkaOffsetPartitionedSegmentRange) segmentRange;

            checkSourceOffsets(ksr);

            sourcePartitionOffsetStart = ksr.getSourcePartitionOffsetStart();
            sourcePartitionOffsetEnd = ksr.getSourcePartitionOffsetEnd();
        }

        final CubeInstance cube = (CubeInstance) buildable;
        if (ksr == null || ksr.getStart().equals(0L)) {
            final CubeSegment last = cube.getLastSegment();
            if (last != null) {
                logger.debug("Last segment exists, continue from last segment " + last.getName() + "'s end position: "
                        + last._getSourcePartitionOffsetEnd());
                // from last seg's end position
                sourcePartitionOffsetStart = last._getSourcePartitionOffsetEnd();
            } else if (cube.getDescriptor().getPartitionOffsetStart() != null
                    && cube.getDescriptor().getPartitionOffsetStart().size() > 0) {
                logger.debug("Last segment doesn't exist, use the start offset that be initiated previously: "
                        + cube.getDescriptor().getPartitionOffsetStart());
                sourcePartitionOffsetStart = cube.getDescriptor().getPartitionOffsetStart();
            } else {
                // from the topic's earliest offset;
                logger.debug(
                        "Last segment doesn't exist, and didn't initiate the start offset, will seek from topic's earliest offset.");
                sourcePartitionOffsetStart = KafkaClient.getEarliestOffsets(cube);
            }
        }

        final KafkaConfig kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getKafkaConfig(cube.getRootFactTable());
        final String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        final String topic = kafkaConfig.getTopic();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cube.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            logger.info("Get {} partitions for topic {} ", partitionInfos.size(), topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                if (!sourcePartitionOffsetStart.containsKey(partitionInfo.partition())) {
                    long earliest = KafkaClient.getEarliestOffset(consumer, topic, partitionInfo.partition());
                    logger.debug("New partition {} added, with start offset {}", partitionInfo.partition(), earliest);
                    sourcePartitionOffsetStart.put(partitionInfo.partition(), earliest);
                }
            }
        }

        if (ksr == null || ksr.getEnd().equals(Long.MAX_VALUE)) {
            logger.debug("Seek end offsets from topic {}", topic);
            Map<Integer, Long> latestOffsets = KafkaClient.getLatestOffsets(cube);
            logger.debug("The end offsets are " + latestOffsets);

            for (Integer partitionId : latestOffsets.keySet()) {
                if (sourcePartitionOffsetStart.containsKey(partitionId)) {
                    if (sourcePartitionOffsetStart.get(partitionId) > latestOffsets.get(partitionId)) {
                        throw new IllegalArgumentException("Partition " + partitionId + " end offset ("
                                + latestOffsets.get(partitionId) + ") is smaller than start offset ( "
                                + sourcePartitionOffsetStart.get(partitionId) + ")");
                    }
                } else {
                    throw new IllegalStateException("New partition added in between, retry.");
                }
            }
            sourcePartitionOffsetEnd = latestOffsets;
        }

        long totalStartOffset = 0, totalEndOffset = 0;
        for (Long v : sourcePartitionOffsetStart.values()) {
            totalStartOffset += v;
        }
        for (Long v : sourcePartitionOffsetEnd.values()) {
            totalEndOffset += v;
        }

        if (totalStartOffset > totalEndOffset) {
            throw new IllegalArgumentException(
                    "Illegal offset: start: " + totalStartOffset + ", end: " + totalEndOffset);
        }

        if (totalStartOffset == totalEndOffset) {
            throw new IllegalArgumentException("No new message comes, startOffset = endOffset:" + totalStartOffset);
        }

        return new SegmentRange.KafkaOffsetPartitionedSegmentRange(totalStartOffset, totalEndOffset,
                sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
    }

    private void checkSourceOffsets(SegmentRange.KafkaOffsetPartitionedSegmentRange ksr) {

        if (ksr == null) {
            return;
        }

        long startOffset = ksr.getStart();
        long endOffset = ksr.getEnd();
        final Map<Integer, Long> sourcePartitionOffsetStart = ksr.getSourcePartitionOffsetStart();
        final Map<Integer, Long> sourcePartitionOffsetEnd = ksr.getSourcePartitionOffsetEnd();
        if (endOffset <= 0 || startOffset >= endOffset) {
            throw new IllegalArgumentException("'startOffset' need be smaller than 'endOffset'");
        }

        if (startOffset > 0) {
            if (sourcePartitionOffsetStart == null || sourcePartitionOffsetStart.size() == 0) {
                throw new IllegalArgumentException(
                        "When 'startOffset' is > 0, need provide each partition's start offset");
            }

            long totalOffset = 0;
            for (Long v : sourcePartitionOffsetStart.values()) {
                totalOffset += v;
            }

            if (totalOffset != startOffset) {
                throw new IllegalArgumentException(
                        "Invalid 'sourcePartitionOffsetStart', doesn't match with 'startOffset'");
            }
        }

        if (endOffset > 0 && endOffset != Long.MAX_VALUE) {
            if (sourcePartitionOffsetEnd == null || sourcePartitionOffsetEnd.size() == 0) {
                throw new IllegalArgumentException(
                        "When 'endOffset' is not Long.MAX_VALUE, need provide each partition's start offset");
            }

            long totalOffset = 0;
            for (Long v : sourcePartitionOffsetEnd.values()) {
                totalOffset += v;
            }

            if (totalOffset != endOffset) {
                throw new IllegalArgumentException(
                        "Invalid 'sourcePartitionOffsetEnd', doesn't match with 'endOffset'");
            }
        }
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new ISourceMetadataExplorer() {

            @Override
            public List<String> listDatabases() throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<String> listTables(String database) throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
                    throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<String> getRelatedKylinResources(TableDesc table) {
                List<String> dependentResources = Lists.newArrayList();
                dependentResources.add(KafkaConfig.concatResourcePath(table.getIdentity()));
                dependentResources.add(StreamingConfig.concatResourcePath(table.getIdentity()));
                return dependentResources;
            }
        };
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        throw new UnsupportedOperationException();
    }

}
