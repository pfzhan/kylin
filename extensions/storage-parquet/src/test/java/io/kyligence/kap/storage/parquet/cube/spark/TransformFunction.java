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
package io.kyligence.kap.storage.parquet.cube.spark;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.sparder.RowSpliter;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.RDDPartitionResult;
import org.apache.spark.sql.execution.datasources.sparder.TestGtscanRequest;

public class TransformFunction implements FlatMapFunction<Iterator<Row>, RDDPartitionResult> {
    GTScanRequest gtScanRequest;
    String scanRequestStr;
    RowSpliter rowSpliter;
    String kylinConf ;


    public TransformFunction(String scanRequestStr, String kylinConf) {
        this.scanRequestStr = scanRequestStr;
        this.kylinConf = kylinConf;
    }

    @Override
    public Iterator<RDDPartitionResult> call(Iterator<Row> internalRowIterator) throws Exception {
        long startTime = System.currentTimeMillis();
        if (gtScanRequest == null) {
            KylinConfig.setKylinConfigInEnvIfMissing(kylinConf);
            gtScanRequest = TestGtscanRequest.serializer.deserialize(ByteBuffer.wrap(scanRequestStr.getBytes("ISO-8859-1")));

        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //todo compress
        while (internalRowIterator.hasNext()) {
            Row row = internalRowIterator.next();
            if(rowSpliter == null){
                rowSpliter = new RowSpliter(row.schema(), gtScanRequest) ;
            }
            rowSpliter.split(row, baos);
        }

        return Collections.singleton(new RDDPartitionResult(baos.toByteArray(), 10000, 10000, 1000, //
                InetAddress.getLocalHost().getHostName(), System.currentTimeMillis()-startTime, 0)).iterator();
    }



}
