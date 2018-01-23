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

package org.apache.kylin.dict.global;

import java.io.IOException;

public class AppendTrieDictionaryHashBuilder extends AppendTrieDictionaryBuilder {

    public AppendTrieDictionaryHashBuilder(String baseDir, int maxEntriesPerSlice, boolean isAppendDictGlobal)
            throws IOException {
        super(baseDir, maxEntriesPerSlice, isAppendDictGlobal);
    }

    public AppendTrieDictionaryHashBuilder(String baseDir, boolean isAppendDictGlobal, int partitions)
            throws IOException {
        super(baseDir, 0, isAppendDictGlobal);
        this.partitions = partitions;
    }

    @Override
    public void addValue(String value) throws IOException {
        int hashKey = getHashKey(value, partitions);
        byte[] valueBytes = bytesConverter.convertToBytes(value);
        byte[] keyBytes = bytesConverter.convertToBytes(String.valueOf(hashKey));

        AppendDictSliceKey key = AppendDictSliceKey.wrap(keyBytes);

        //key changed, build next hash key slice
        if (!key.equals(curKey)) {
            if (curNode != null)
                flushCurrentNode();
            initializeNode(key);
            curKey = key;
        }

        addValueR(curNode, valueBytes, 0);
        maxValueLength = Math.max(maxValueLength, valueBytes.length);
    }

    private void initializeNode(AppendDictSliceKey key) throws IOException {
        if (sliceFileMap.keySet().contains(key) && sliceFileMap.get(key) != null) {
            AppendDictSlice slice = store.readSlice(workingDir, sliceFileMap.get(key));
            curNode = slice.rebuildTrieTree();
        } else {
            curNode = new AppendDictNode(new byte[0], false);
            sliceFileMap.put(key, null);
        }
    }

    public static int getHashKey(String value, int partitions) {
        int hashcode = value.hashCode();
        return Math.abs(hashcode % partitions);
    }
}
