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

package io.kyligence.kap.engine.mr.tablestats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

public class HiveTableExtMapper<T> extends KylinMapper<T, Object, IntWritable, BytesWritable> {
    private Map<Integer, HiveTableExtSampler> samplerMap = new HashMap<>();

    private long counter = 0;
    private TableDesc tableDesc;
    private IMRInput.IMRTableInputFormat tableInputFormat;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String tableName = conf.get(BatchConstants.CFG_TABLE_NAME);
        tableDesc = MetadataManager.getInstance(config).getTableDesc(tableName);
        tableInputFormat = MRUtil.getTableInputFormat(tableDesc);
        ColumnDesc[] columns = tableDesc.getColumns();
        for (int i = 0; i < columns.length; i++) {
            HiveTableExtSampler sampler = new HiveTableExtSampler();
            sampler.setDataType(columns[i].getType().getName());
            sampler.setColumnName(columns[i].getName());
            samplerMap.put(i, sampler);
        }
    }

    @Override
    public void doMap(T key, Object value, Context context) throws IOException, InterruptedException {
        ColumnDesc[] columns = tableDesc.getColumns();
        String[] values = tableInputFormat.parseMapperInput(value);
        for (int m = 0; m < columns.length; m++) {
            String fieldValue = values[m];
            samplerMap.get(m).samples(fieldValue, counter);
        }
        counter++;
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        Iterator<Integer> it = samplerMap.keySet().iterator();
        while (it.hasNext()) {
            int key = it.next();
            HiveTableExtSampler sampler = samplerMap.get(key);
            sampler.setCounter(String.valueOf(counter));
            sampler.code();
            context.write(new IntWritable(key), new BytesWritable(sampler.getBuffer().array(), sampler.getBuffer().limit()));
        }
    }
}