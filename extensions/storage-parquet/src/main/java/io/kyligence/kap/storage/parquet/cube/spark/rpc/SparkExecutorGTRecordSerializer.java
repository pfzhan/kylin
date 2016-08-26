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

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;

//not thread safe!
public class SparkExecutorGTRecordSerializer implements com.google.common.base.Function<GTRecord, ByteArray> {
    private ImmutableBitSet columns;
    private ByteArray buffer;//shared

    public SparkExecutorGTRecordSerializer(GTScanRequest gtScanRequest, ImmutableBitSet columns) {
        this.columns = columns;
        this.buffer = ByteArray.allocate(gtScanRequest.getInfo().getMaxLength());
    }

    @Nullable
    @Override
    public ByteArray apply(@Nullable GTRecord input) {
        input.exportColumns(columns, buffer);
        return buffer;
    }
}
