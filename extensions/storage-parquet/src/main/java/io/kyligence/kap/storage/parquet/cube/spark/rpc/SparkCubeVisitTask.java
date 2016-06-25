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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.api.client.util.Maps;
import com.google.common.collect.Iterables;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.OriginalBytesGTScanner;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawInputFormat;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.pageIndex.format.ParquetPageIndexInputFormat;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
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

    private static String serializeHashMap(HashMap<String, SerializableImmutableRoaringBitmap> map) {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        String result = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(map);
            result = new String(bos.toByteArray(), "ISO-8859-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(oos);
            IOUtils.closeQuietly(bos);
        }
        return result;
    }

    public List<byte[]> executeTask() {
        String basePath = new StringBuffer(kylinConfig.getHdfsWorkingDirectory()).append("parquet/").//
                append(bcCanonicalCuboid.getValue().getCubeId()).append("/").//
                append(bcCanonicalCuboid.getValue().getSegmentId()).append("/").//
                append(bcCanonicalCuboid.getValue().getCuboidId()).toString();

        String parquetInvertedIndexPath = basePath + "/*.parquet.inv";
        String parquetPath = basePath + "/*.parquet";

        Configuration conf = new Configuration();

        // lookup in parquet inverted index
        System.out.println("================Inverted Index Start==================");
        System.out.println("Parquet inverted index path is " + parquetInvertedIndexPath);
        JavaPairRDD<String, ParquetPageIndexTable> indexSeed = sc.newAPIHadoopFile(parquetInvertedIndexPath, ParquetPageIndexInputFormat.class, String.class, ParquetPageIndexTable.class, conf);
        JavaRDD<Tuple2<String, SerializableImmutableRoaringBitmap>> indexRdd = indexSeed.map(new Function<Tuple2<String, ParquetPageIndexTable>, Tuple2<String, SerializableImmutableRoaringBitmap>>() {
            @Override
            public Tuple2<String, SerializableImmutableRoaringBitmap> call(Tuple2<String, ParquetPageIndexTable> tuple) throws Exception {
                GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(bcGtReq.getValue()));//TODO avoid ByteString's array copy
                TupleFilter filter = gtScanRequest.getFilterPushDown();
                SerializableImmutableRoaringBitmap result = new SerializableImmutableRoaringBitmap(tuple._2().lookup(filter));
                return new Tuple2<>(tuple._1(), result);
            }
        });
        List<Tuple2<String, SerializableImmutableRoaringBitmap>> indexTuples = indexRdd.collect();
        HashMap<String, SerializableImmutableRoaringBitmap> indexMap = Maps.newHashMap();
        for (Tuple2<String, SerializableImmutableRoaringBitmap> tuple : indexTuples) {
            indexMap.put(tuple._1(), tuple._2());
        }
        // print index results
        System.out.println("Index result size: " + indexMap.size());
        for (Map.Entry<String, SerializableImmutableRoaringBitmap> indexEntry : indexMap.entrySet()) {
            System.out.println("Inverted Index: path=" + indexEntry.getKey() + ", bitmap=" + indexEntry.getValue());
        }
        conf.set(ParquetFormatConstants.KYLIN_FILTER_PAGE_BITSET_MAP, serializeHashMap(indexMap));
        System.out.println("================Inverted Index End==================");

        // visit parquet data file
        System.out.println("Parquet path is " + parquetPath);
        JavaPairRDD<Text, Text> seed = sc.newAPIHadoopFile(parquetPath, ParquetRawInputFormat.class, Text.class, Text.class, conf);
        List<byte[]> collected = seed.map(new Function<Tuple2<Text, Text>, byte[]>() {
            @Override
            public byte[] call(Tuple2<Text, Text> tuple) throws Exception {
                //                System.out.println("Key: " + tuple._1().getBytes());
                //                System.out.println("Value: " + tuple._2().getBytes());

                return tuple._2().getBytes();
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
