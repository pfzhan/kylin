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

import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.kylin.common.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;

public class JobServiceImpl implements JobServiceGrpc.JobService {

    SparkConf conf;
    JavaSparkContext sc;

    public JobServiceImpl() {
        conf = new SparkConf().setAppName("Kylin Parquet Storage Query Driver");
        sc = new JavaSparkContext(conf);
    }

    @Override
    public void submitJob(SparkJobRequest request, StreamObserver<SparkJobResponse> responseObserver) {

        int reqValue = Bytes.toInt(request.getRequest().toByteArray());
        List<Integer> values = Lists.newArrayList();
        for (int i = 0; i < reqValue; i++) {
            values.add(i + 1);
        }

        JavaRDD<Integer> data = sc.parallelize(values);
        long result = data.count();
        System.out.println("The result is " + result);

//        int reqValue = Bytes.toInt(request.getRequest().toByteArray());
//        System.out.println("reqValue is " + reqValue);
        SparkJobResponse response = SparkJobResponse.newBuilder().setResponse(ByteString.copyFrom(Bytes.toBytes(result))).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
