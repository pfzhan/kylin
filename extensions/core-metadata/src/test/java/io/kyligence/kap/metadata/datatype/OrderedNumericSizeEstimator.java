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

package io.kyligence.kap.metadata.datatype;

import java.util.List;

import org.apache.hadoop.hbase.types.OrderedNumeric;
import org.junit.Test;

import com.google.common.collect.Lists;

public class OrderedNumericSizeEstimator {
    @Test
    public void estimate() {
        OrderedNumeric ascending = OrderedNumeric.ASCENDING;
        List<Number> sampleData = Lists.newArrayList();
        sampleData.add(null);
        sampleData.add(0);

        sampleData.add(0.3);
        sampleData.add(0.93);
        sampleData.add(0.003241);
        sampleData.add(1.5);
        sampleData.add(1.90);
        sampleData.add(31.90);
        sampleData.add(9931.90);
        sampleData.add(9931.9332);
        sampleData.add(1209931.9332);
        sampleData.add(99991209931.9332);
        sampleData.add(-0.3);
        sampleData.add(-0.93);
        sampleData.add(-0.003241);
        sampleData.add(-1.5);
        sampleData.add(-1.90);
        sampleData.add(-31.90);
        sampleData.add(-9931.90);
        sampleData.add(-9931.9332);
        sampleData.add(-1209931.9332);
        sampleData.add(-99991209931.9332);

        int totalLength = 0;
        for (Number number : sampleData) {
            int i = ascending.encodedLength(number);
            System.out.println("Number " + number + " length is : " + i);

            totalLength += i;
        }
        System.out.println("AVG: " + (totalLength / sampleData.size()));
    }
}
