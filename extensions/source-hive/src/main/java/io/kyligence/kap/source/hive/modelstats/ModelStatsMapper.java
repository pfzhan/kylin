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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.HiveMRInput;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampler;

public class ModelStatsMapper<T> extends KylinMapper<T, Object, IntWritable, BytesWritable> {

    private Map<Integer, HiveTableExtSampler> samplerMap = new HashMap<>();

    private int counter = 0;

    private DataModelDesc dataModelDesc;
    private IMRInput.IMRTableInputFormat tableInputFormat;
    private IJoinedFlatTableDesc flatTableDesc;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String model = conf.get(BatchConstants.CFG_TABLE_NAME);
        dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(model);
        flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);
        String fullTableName = config.getHiveDatabaseForIntermediateTable() + "." + flatTableDesc.getTableName();
        tableInputFormat = new HiveMRInput.HiveTableInputFormat(fullTableName);
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        int sampleFrequency = kapConfig.getStatsSampleFrequency();

        List<TblColRef> columns = flatTableDesc.getAllColumns();
        for (int i = 0; i < columns.size(); i++) {
            HiveTableExtSampler sampler = new HiveTableExtSampler(i, columns.size());
            sampler.setDataType(columns.get(i).getType().getName());
            sampler.setColumnName(columns.get(i).getCanonicalName());
            sampler.setStatsSampleFrequency(sampleFrequency);
            samplerMap.put(i, sampler);
        }
    }

    @Override
    public void doMap(T key, Object value, Context context) throws IOException, InterruptedException {
        String[] values = tableInputFormat.parseMapperInput(value);
        for (int m = 0; m < flatTableDesc.getAllColumns().size(); m++) {
            samplerMap.get(m).samples(values);
        }
        counter++;
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        Iterator<Integer> it = samplerMap.keySet().iterator();
        while (it.hasNext()) {
            int key = it.next();
            HiveTableExtSampler sampler = samplerMap.get(key);
            sampler.sync();
            ByteBuffer buf = sampler.code();
            context.write(new IntWritable(key), new BytesWritable(buf.array(), buf.position()));
        }
    }
}