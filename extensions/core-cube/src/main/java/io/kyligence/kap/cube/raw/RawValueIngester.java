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

package io.kyligence.kap.cube.raw;

import org.apache.kylin.common.util.ImmutableBitSet;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.datatype.OrderedBytesStringSerializer;

public class RawValueIngester {

    //TODO: null values are silently transformed to default values
    public static Object[] buildObjectOf(String[] values, BufferedRawColumnCodec columnCodec, ImmutableBitSet activeCols) {
        Preconditions.checkArgument(values.length == columnCodec.getColumnsCount());

        Object[] objects = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            if (activeCols.get(i)) {
                //special treatment for hive null values, only string will reserve null values
                //other types will be changed to 0
                if (!"\\N".equals(values[i])) {
                    objects[i] = columnCodec.getDataTypeSerializer(i).valueOf(values[i]);
                } else {
                    if (columnCodec.getDataTypeSerializer(i) instanceof OrderedBytesStringSerializer) {
                        objects[i] = null;
                    } else {
                        objects[i] = columnCodec.getDataTypeSerializer(i).valueOf("0");
                    }
                }
            }
        }
        return objects;
    }
}
