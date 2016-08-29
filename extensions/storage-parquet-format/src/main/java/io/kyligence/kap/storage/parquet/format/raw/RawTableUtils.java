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

package io.kyligence.kap.storage.parquet.format.raw;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;

public class RawTableUtils {
    public static List<byte[]> hash(List<byte[]> value) {
        List<byte[]> result = new ArrayList<>(value.size());
        for (byte[] v : value) {
            result.add(hash(v));
        }
        return result;
    }

    public static byte[] hash(byte[] value) {
        byte[] result = new byte[8];

        if (value.length <= 8) {
            System.arraycopy(value, 0, result, 0, value.length);
            for (int i = value.length; i < 8; i++) {
                result[i] = (byte) 0;
            }
        } else {
            System.arraycopy(value, 0, result, 0, 8);

            for (int i = 8; i < value.length; i++) {
                result[i % 8] ^= value[i];
            }
        }

        return result;
    }

    public static ByteArray hash(ByteArray value) {
        int hashLength= KapConfig.getInstanceFromEnv().getParquetIndexHashLength();
        byte[] result = new byte[hashLength];

        if (value.length() <= hashLength) {
            System.arraycopy(value.array(), value.offset(), result, 0, value.length());
            for (int i = value.length(); i < hashLength; i++) {
                result[i] = (byte) 0;
            }
        } else {
            System.arraycopy(value.array(), value.offset(), result, 0, hashLength);

            for (int i = hashLength; i < value.length(); i++) {
                result[i % hashLength] ^= value.array()[i + value.offset()];
            }
        }

        return new ByteArray(result);
    }
}
