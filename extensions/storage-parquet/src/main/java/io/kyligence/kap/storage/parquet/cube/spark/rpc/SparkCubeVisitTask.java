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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.OriginalBytesGTScanner;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Iterables;

public class SparkCubeVisitTask implements Serializable {
    private JavaRDD<Integer> seed;
    private Broadcast<byte[]> bcGtReq;
    private Broadcast<String> kylinProperties;

    public SparkCubeVisitTask(JavaRDD<Integer> seed, Broadcast<byte[]> bcGtReq, Broadcast<String> kylinProperties) {
        this.seed = seed;
        this.bcGtReq = bcGtReq;
        this.kylinProperties = kylinProperties;
    }

    public List<byte[]> executeTask() {
        List<byte[]> collected = seed.map(new Function<Integer, byte[]>() {
            @Override
            public byte[] call(Integer integer) throws Exception {
                return new byte[] { 0, 0, 1, 74, -27, -67, 92, 0, 2, 9, 10, 1, 1, 105 };
            }
        }).mapPartitions(new FlatMapFunction<Iterator<byte[]>, byte[]>() {

            @Override
            public Iterable<byte[]> call(final Iterator<byte[]> iterator) throws Exception {

                // if user change kylin.properties on kylin server, need to manually redeploy coprocessor jar to update KylinConfig of Env.
                KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(kylinProperties.getValue()));
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

                GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(bcGtReq.getValue()));//TODO avoid ByteString's array copy

                IGTScanner scanner = new OriginalBytesGTScanner(gtScanRequest.getInfo(), iterator);//in
                IGTScanner preAggred = gtScanRequest.decorateScanner(scanner);//process
                return Iterables.transform(preAggred, new CoalesceGTRecordExport(gtScanRequest));//out
            }
        }).collect();

        System.out.println("The result size is " + collected.size());
        return collected;
    }
}
