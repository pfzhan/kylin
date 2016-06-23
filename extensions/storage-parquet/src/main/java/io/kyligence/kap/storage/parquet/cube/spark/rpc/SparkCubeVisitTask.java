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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Iterables;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.OriginalBytesGTScanner;
import io.kyligence.kap.storage.parquet.format.ParquetRawInputFormat;
import scala.Tuple2;

public class SparkCubeVisitTask implements Serializable {

    private transient JavaSparkContext sc;
    private transient KylinConfig kylinConfig;

    private Broadcast<byte[]> bcGtReq;
    private Broadcast<String> bcKylinProperties;
    private Broadcast<CanonicalCuboid> bcCanonicalCuboid;

    public SparkCubeVisitTask(JavaSparkContext sc, SparkJobProtos.SparkJobRequest request) {
        this.sc = sc;
        this.kylinConfig = KylinConfig.createKylinConfigFromInputStream(IOUtils.toInputStream(request.getKylinProperties()));

        this.bcGtReq = sc.broadcast(request.getGtScanRequest().toByteArray());
        this.bcKylinProperties = sc.broadcast(request.getKylinProperties());
        this.bcCanonicalCuboid = sc.broadcast(new CanonicalCuboid(request.getCubeId(), request.getSegmentId(), request.getCuboidId()));
    }

    public List<byte[]> executeTask() {

        //visit index
        //        ImmutableRoaringBitmap bitmap = createBitset(3);
        //        String serializedString = serialize(bitmap);
        //        System.out.println("serialized String size: " + serializedString.length());
        //        config.set(ParquetFormatConstants.KYLIN_FILTER_BITSET, serializedString);

        // Create and
        String path = new StringBuffer(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                append(bcCanonicalCuboid.getValue().getCubeId()).append("/").//
                append(bcCanonicalCuboid.getValue().getSegmentId()).append("/").//
                append(bcCanonicalCuboid.getValue().getCuboidId()).append("/*.parquet").toString();

        JavaPairRDD<Text, Text> seed = sc.newAPIHadoopFile(path, ParquetRawInputFormat.class, Text.class, Text.class, new Configuration());

        List<byte[]> collected = seed.map(new Function<Tuple2<Text, Text>, byte[]>() {
            @Override
            public byte[] call(Tuple2<Text, Text> tuple) throws Exception {
                //                System.out.println("Key: " + tuple._1().getBytes());
                //                System.out.println("Value: " + tuple._2().getBytes());
                byte[] temp = new byte[tuple._1.getBytes().length + tuple._2.getBytes().length];
                System.arraycopy(tuple._1, 0, temp, 0, tuple._1.getBytes().length);
                System.arraycopy(tuple._2, 0, temp, tuple._1.getBytes().length, tuple._2.getBytes().length);
                return temp;
            }
        }).mapPartitions(new FlatMapFunction<Iterator<byte[]>, byte[]>() {

            @Override
            public Iterable<byte[]> call(final Iterator<byte[]> iterator) throws Exception {

                // if user change kylin.properties on kylin server, need to manually redeploy coprocessor jar to update KylinConfig of Env.
                KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(bcKylinProperties.getValue()));
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
