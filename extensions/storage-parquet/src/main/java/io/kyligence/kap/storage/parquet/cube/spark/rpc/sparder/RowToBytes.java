/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult;
  @Deprecated
public class RowToBytes implements FlatMapFunction<Iterator<Row>, RDDPartitionResult> {
    int max = -1;

    public RowToBytes() {
    }

    @Override
    public Iterator<RDDPartitionResult> call(Iterator<Row> internalRowIterator) throws Exception {
        long startTime = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //todo compress
        while (internalRowIterator.hasNext()) {
            Row row = internalRowIterator.next();
            if (max == -1) {
                max = row.schema().size();
            }
            for (int i = 0; i < max; i++) {
                byte[] apply = (byte[]) row.apply(i);
                baos.write(apply);
            }
        }

        return Collections.singleton(new RDDPartitionResult(baos.toByteArray(), 10000, 10000, 1000, //
                InetAddress.getLocalHost().getHostName(), System.currentTimeMillis() - startTime, 0)).iterator();
    }

}
