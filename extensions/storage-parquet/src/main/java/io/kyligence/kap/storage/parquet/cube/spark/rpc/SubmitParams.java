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

public class SubmitParams {
    private final String kylinProperties;
    private final String cubeId;
    private final String segmentId;
    private final String cuboidId;
    private final int maxGTLength;
    private final List<Integer> requiredMeasures;

    public SubmitParams(String kylinProperties, String cubeId, String segmentId, String cuboidId, int maxGTLength, List<Integer> requiredMeasures) {
        this.kylinProperties = kylinProperties;
        this.cubeId = cubeId;
        this.segmentId = segmentId;
        this.cuboidId = cuboidId;
        this.maxGTLength = maxGTLength;
        this.requiredMeasures = requiredMeasures;
    }

    public String getKylinProperties() {
        return kylinProperties;
    }

    public String getCubeId() {
        return cubeId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public String getCuboidId() {
        return cuboidId;
    }

    public int getMaxGTLength() {
        return maxGTLength;
    }

    public List<Integer> getRequiredMeasures() {
        return requiredMeasures;
    }
}
