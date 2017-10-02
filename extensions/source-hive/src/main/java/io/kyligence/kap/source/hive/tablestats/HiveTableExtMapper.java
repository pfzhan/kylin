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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableExtMapper<T> extends KylinMapper<T, Object, IntWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtMapper.class);

    private Map<Integer, HiveTableExtSampler> samplerMap = new HashMap<>();

    private boolean isOffsetZero = false;
    private int skipHeaderLineCount = 0;
    private TableDesc tableDesc;
    private IMRInput.IMRTableInputFormat tableInputFormat;

    @Override
    protected void doSetup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        HCatSplit hCatSplit = (HCatSplit) context.getInputSplit();
        FileSplit fileSplit = (FileSplit) hCatSplit.getBaseSplit();
        isOffsetZero = (0 == fileSplit.getStart());
        String skipCount = conf.get("skip.header.line.count");
        if (null != skipCount)
            skipHeaderLineCount = Integer.parseInt(skipCount);

        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String tableName = conf.get(BatchConstants.CFG_TABLE_NAME);
        String project = conf.get(BatchConstants.CFG_PROJECT_NAME);
        int frequency = Integer.parseInt(conf.get(BatchConstants.CFG_STATS_JOB_FREQUENCY));
        tableDesc = TableMetadataManager.getInstance(config).getTableDesc(tableName, project);
        tableInputFormat = MRUtil.getTableInputFormat(tableDesc);
        ColumnDesc[] columns = tableDesc.getColumns();
        for (int i = 0; i < columns.length; i++) {
            HiveTableExtSampler sampler = new HiveTableExtSampler(columns[i].getType().getName(),
                    columns[i].getType().getPrecision());
            sampler.setColumnName(columns[i].getName());
            sampler.setStatsSampleFrequency(frequency);
            samplerMap.put(i, sampler);
        }
    }

    @Override
    public void doMap(T key, Object value, Context context) throws IOException, InterruptedException {
        if (skipHeaderLineCount > 0 && isOffsetZero) {
            skipHeaderLineCount--;
            return;
        }
        ColumnDesc[] columns = tableDesc.getColumns();
        Collection<String[]> valuesCollection = tableInputFormat.parseMapperInput(value);

        for (String[] values : valuesCollection) {
            for (int m = 0; m < columns.length; m++) {
                try {
                    String fieldValue = values[m];
                    samplerMap.get(m).samples(fieldValue);
                } catch (Exception e) {
                    logger.error("error when handling sampled data for column {} ", tableDesc.getColumns()[m]);
                    throw e;
                }
            }
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        Iterator<Integer> it = samplerMap.keySet().iterator();
        while (it.hasNext()) {
            int key = it.next();
            try {
                HiveTableExtSampler sampler = samplerMap.get(key);
                sampler.sync();
                ByteBuffer buf = sampler.code();
                context.write(new IntWritable(key), new BytesWritable(buf.array(), buf.position()));
                sampler.clean();
            } catch (Exception e) {
                logger.error("error when handling sampled data for column {} ", tableDesc.getColumns()[key]);
                throw e;
            }
        }
    }
}
