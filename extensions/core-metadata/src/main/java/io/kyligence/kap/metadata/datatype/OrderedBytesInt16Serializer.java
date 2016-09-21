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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.types.OrderedInt16;
import org.apache.kylin.metadata.datatype.DataType;

public class OrderedBytesInt16Serializer extends OrderedBytesSerializer<Short> {

    public OrderedBytesInt16Serializer(DataType type) {
        super(type);
        orderedBytesBase = OrderedInt16.ASCENDING;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 3;
    }

    @Override
    public int maxLength() {
        return 3;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 3;
    }

    public Short valueOf(String str) {
        if (str == null)
            return 0;
        else
            return Short.parseShort(str);
    }

}
