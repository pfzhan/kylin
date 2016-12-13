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

package io.kyligence.kap.engine.mr.modelstats;

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
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;
import io.kyligence.kap.engine.mr.tablestats.HiveTableExtSampler;

public class ModelStatsReducer extends KylinReducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

    private Map<Integer, HiveTableExtSampler> samplerMap = new HashMap<Integer, HiveTableExtSampler>();

    private int columnSize = 0;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        Configuration conf = context.getConfiguration();
        String model = conf.get(BatchConstants.CFG_TABLE_NAME);
        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(model);
        IJoinedFlatTableDesc flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);
        columnSize = flatTableDesc.getAllColumns().size();
    }

    @Override
    public void doReduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        int skey = key.get();
        for (BytesWritable v : values) {
            ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());
            HiveTableExtSampler sampler = new HiveTableExtSampler(key.get(), columnSize);
            sampler.decode(buffer);
            if (!samplerMap.containsKey(skey))
                samplerMap.put(skey, sampler);
            else {
                samplerMap.get(skey);
            }
            samplerMap.get(skey).merge(sampler);
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
            HiveTableExtSampler sampler = samplerMap.get(key);
            sampler.code();
            context.write(new IntWritable(key), new BytesWritable(sampler.getBuffer().array(), sampler.getBuffer().limit()));
            sampler.clean();
        }
    }
}