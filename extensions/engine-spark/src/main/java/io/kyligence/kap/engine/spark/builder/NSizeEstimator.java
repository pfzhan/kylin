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

package io.kyligence.kap.engine.spark.builder;

import static org.apache.spark.sql.functions.sum;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Preconditions;

public class NSizeEstimator {
    public static long estimate(Dataset<Row> ds, float ratio) {
        if (ratio < 0.0001f)
            return 0;

        final int frequency = (int) (1 / ratio);

        StructType schema = new StructType();
        schema = schema.add("RowSize", DataTypes.LongType, false);
        List<Row> ret = ds.map(new MapFunction<Row, Row>() {
            private transient long count = 0;

            @Override
            public Row call(Row value) throws Exception {
                long size = 0;
                if (count % frequency == 0) {
                    for (int i = 0; i < value.size(); i++) {
                        size += value.get(i).toString().getBytes().length;
                    }
                }
                count++;
                return RowFactory.create(size);
            }
        }, org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(schema)).agg(sum("RowSize")).collectAsList();

        Preconditions.checkArgument(ret.size() == 1);
        return ret.get(0).getLong(0) * frequency;
    }
}
