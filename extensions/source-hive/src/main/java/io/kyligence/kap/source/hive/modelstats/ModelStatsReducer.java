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

package io.kyligence.kap.source.hive.modelstats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampler;

public class ModelStatsReducer extends KylinReducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

    private static final Logger logger = LoggerFactory.getLogger(ModelStatsMapper.class);

    private Map<Integer, HiveTableExtSampler> samplerMap = new HashMap<Integer, HiveTableExtSampler>();

    private int columnSize = 0;
    List<TblColRef> columns;

    private IJoinedFlatTableDesc flatTableDesc;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        Configuration conf = context.getConfiguration();
        String model = conf.get(BatchConstants.CFG_TABLE_NAME);
        DataModelDesc dataModelDesc = DataModelManager.getInstance(config).getDataModelDesc(model);
        flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);
        columnSize = flatTableDesc.getAllColumns().size();
        columns = flatTableDesc.getAllColumns();
    }

    @Override
    public void doReduce(IntWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        int skey = key.get();
        try {
            for (BytesWritable v : values) {
                ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());
                String type = columns.get(skey).getType().getName();
                int precision = columns.get(skey).getType().getPrecision();
                HiveTableExtSampler sampler = new HiveTableExtSampler(type, precision, key.get(), columnSize);
                sampler.decode(buffer);
                if (!samplerMap.containsKey(skey))
                    samplerMap.put(skey, sampler);
                else {
                    samplerMap.get(skey);
                }
                samplerMap.get(skey).merge(sampler);
            }
        } catch (Exception e) {
            logger.error("error when handling sampled data for column {} ", flatTableDesc.getAllColumns().get(skey));
            throw e;
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        List<Integer> keys = new ArrayList<Integer>();
        Iterator<Integer> it = samplerMap.keySet().iterator();
        while (it.hasNext()) {
            keys.add(it.next());
        }
        Collections.sort(keys);
        it = keys.iterator();
        while (it.hasNext()) {
            int key = it.next();
            try {
                HiveTableExtSampler sampler = samplerMap.get(key);
                context.write(new IntWritable(key), new BytesWritable(sampler.code().array(), sampler.code().limit()));
                sampler.clean();
            } catch (Exception e) {
                logger.error("error when handling sampled data for column {} ", flatTableDesc.getAllColumns().get(key));
                throw e;
            }
        }
    }
}
