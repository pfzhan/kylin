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

package io.kyligence.kap.engine.mr.steps;

import static io.kyligence.kap.engine.mr.steps.ConfigurationBasedPartitioner.getByteArrayPartition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayConfigurationBasedPartitioner extends Partitioner<ByteArrayWritable, ByteArrayWritable>
        implements Configurable {
    protected static final Logger logger = LoggerFactory.getLogger(ByteArrayConfigurationBasedPartitioner.class);

    public static final String CUBOID_SHARD_REDUCE_MAPPING = "io.kyligence.kap.mr.partitioner-mapping";
    private Configuration conf = null;
    private Map<Pair<Long, Short>, Integer> partitionMap = null;

    @Override
    public int getPartition(ByteArrayWritable key, ByteArrayWritable value, int numReduceTasks) {
        return getByteArrayPartition(key.array(), value.array(), numReduceTasks, partitionMap);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        try {
            String partitionMapping = conf.get(CUBOID_SHARD_REDUCE_MAPPING);
            if (partitionMapping != null) {
                partitionMap = (Map<Pair<Long, Short>, Integer>) new ObjectInputStream(
                        new ByteArrayInputStream(Base64.decodeBase64(partitionMapping.getBytes()))).readObject();
            }

            for (Pair<Long, Short> key : partitionMap.keySet()) {
                logger.info("Cuboid {} Shard {} --> Reducer Number {}", key.getFirst(), key.getSecond(),
                        partitionMap.get(key));
            }
        } catch (IOException e) {
            logger.error("", e);
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
