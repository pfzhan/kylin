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

package io.kyligence.kap.storage.parquet.cube.spark;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.JobResponseBlobGTScanner;

public class CubeSparkRPC implements IGTStorage {
    private CubeSegment cubeSegment;
    private Cuboid cuboid;
    private GTInfo info;

    private SparkDriverClient client;

    public CubeSparkRPC(CubeSegment cubeSegment, Cuboid cuboid, GTInfo info) {
        this.cubeSegment = cubeSegment;
        this.cuboid = cuboid;
        this.info = info;

        client = new SparkDriverClient("localhost", 50055);
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequests) throws IOException {
        SparkJobProtos.SparkJobResponse jobResponse = client.submit( //
                scanRequests.toByteArray(), //
                KylinConfig.getInstanceFromEnv().getConfigAsString(), //
                cubeSegment.getCubeInstance().getName(), //
                cubeSegment.getName(), //
                String.valueOf(cuboid.getId()) //
        );
        return new JobResponseBlobGTScanner(scanRequests, jobResponse.getGtRecordsBlob().toByteArray());
    }
}
