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
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.OriginalBytesGTScanner;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetRawRecordReader;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import scala.Tuple2;

public class SparkCubeVisitJob implements Serializable {

    private transient JavaSparkContext sc;
    private transient SparkJobProtos.SparkJobRequest request;
    private transient KylinConfig kylinConfig;

    //    private Broadcast<byte[]> bcGtReq;
    //    private Broadcast<String> bcKylinProperties;
    private Broadcast<CanonicalCuboid> bcCanonicalCuboid;

    public SparkCubeVisitJob(JavaSparkContext sc, SparkJobProtos.SparkJobRequest request) {
        this.sc = sc;
        this.request = request;
        this.kylinConfig = KylinConfig.createKylinConfigFromInputStream(IOUtils.toInputStream(request.getKylinProperties()));

        //this.bcGtReq = sc.broadcast(request.getGtScanRequest().toByteArray());
        //this.bcKylinProperties = sc.broadcast(request.getKylinProperties());
        this.bcCanonicalCuboid = sc.broadcast(new CanonicalCuboid(request.getCubeId(), request.getSegmentId(), request.getCuboidId()));
    }

    private static String serializeBitMap(Iterable<Integer> bits) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i : bits) {
            bitmap.add(i);
        }
        ByteArrayOutputStream bos = null;
        DataOutputStream dos = null;
        String result = null;
        try {
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            bitmap.serialize(dos);
            dos.flush();
            result = new String(bos.toByteArray(), "ISO-8859-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
        }
        return result;
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

        String parquetPath = basePath + "/*.parquet";

        Configuration conf = new Configuration();

        System.out.println("Required Measures: " + StringUtils.join(request.getRequiredMeasuresList(), ","));
        conf.set(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP, serializeBitMap(request.getRequiredMeasuresList()));

        System.out.println("Max GT length: " + request.getMaxRecordLength());
        conf.set(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH, String.valueOf(request.getMaxRecordLength()));

        conf.set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, request.getKylinProperties());
        try {
            //so that ParquetRawInputFormat can use the scan request
            conf.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES, new String(this.request.getGtScanRequest().toByteArray(), "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        System.out.println("================Cube Data Start==================");
        // visit parquet data file
        System.out.println("Parquet path is " + parquetPath);
        JavaPairRDD<byte[], byte[]> seed = sc.newAPIHadoopFile(parquetPath, ParquetRawInputFormat.class, byte[].class, byte[].class, conf);
        List<byte[]> collected = seed.mapPartitions(new FlatMapFunction<Iterator<Tuple2<byte[], byte[]>>, byte[]>() {
            @Override
            public Iterable<byte[]> call(Iterator<Tuple2<byte[], byte[]>> tuple2Iterator) throws Exception {
                Iterator<byte[]> iterator = Iterators.transform(tuple2Iterator, new com.google.common.base.Function<Tuple2<byte[], byte[]>, byte[]>() {
                    @Nullable
                    @Override
                    public byte[] apply(@Nullable Tuple2<byte[], byte[]> input) {
                        return input._2;
                    }
                });

                //                // if user change kylin.properties on kylin server, need to manually redeploy coprocessor jar to update KylinConfig of Env.
                //                KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(bcKylinProperties.getValue()));
                //                GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(bcGtReq.getValue()));//TODO avoid ByteString's array copy
                GTScanRequest gtScanRequest = ParquetRawRecordReader.gtScanRequestThreadLocal.get();

                IGTScanner scanner = new OriginalBytesGTScanner(gtScanRequest.getInfo(), iterator);//in
                IGTScanner preAggred = gtScanRequest.decorateScanner(scanner);//process
                return Iterables.transform(preAggred, new CoalesceGTRecordExport(gtScanRequest));//out
            }
        }).collect();
        System.out.println("================Cube Data End==================");

        System.out.println("The result size is " + collected.size());
        return collected;
    }
}
