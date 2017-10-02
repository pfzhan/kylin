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

package io.kyligence.kap.source.hive.tablestats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableExtReducer extends KylinReducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtReducer.class);

    private Map<Integer, HiveTableExtSampler> sampleMap = new HashMap<Integer, HiveTableExtSampler>();
    private TableDesc tableDesc;
    private Map<Integer, DataType> dataTypeMap = new HashMap<>();

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String project = context.getConfiguration().get(BatchConstants.CFG_PROJECT_NAME);
        String tableName = context.getConfiguration().get(BatchConstants.CFG_TABLE_NAME);
        tableDesc = TableMetadataManager.getInstance(config).getTableDesc(tableName, project);
        ColumnDesc[] columns = tableDesc.getColumns();
        for (int i = 0; i < columns.length; i++) {
            dataTypeMap.put(i, columns[i].getType());
        }
    }

    @Override
    public void doReduce(IntWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        int skey = key.get();
        try {
            for (BytesWritable v : values) {
                ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());
                HiveTableExtSampler sampler = new HiveTableExtSampler(dataTypeMap.get(skey).getName(),
                        dataTypeMap.get(skey).getPrecision());
                sampler.decode(buffer);
                if (!sampleMap.containsKey(skey))
                    sampleMap.put(skey, sampler);
                else {
                    sampleMap.get(skey);
                }
                sampleMap.get(skey).merge(sampler);
            }
        } catch (Exception e) {
            logger.error("error when handling sampled data for column {} ", tableDesc.getColumns()[skey]);
            throw e;
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        List<Integer> keys = new ArrayList<Integer>();
        Iterator<Integer> it = sampleMap.keySet().iterator();
        while (it.hasNext()) {
            keys.add(it.next());
        }
        Collections.sort(keys);
        it = keys.iterator();
        while (it.hasNext()) {
            int key = it.next();
            try {
                HiveTableExtSampler sampler = sampleMap.get(key);
                context.write(new IntWritable(key), new BytesWritable(sampler.code().array(), sampler.code().limit()));
                sampler.clean();
            } catch (Exception e) {
                logger.error("error when handling sampled data for column {} ", tableDesc.getColumns()[key]);
                throw e;
            }
        }
    }
}
