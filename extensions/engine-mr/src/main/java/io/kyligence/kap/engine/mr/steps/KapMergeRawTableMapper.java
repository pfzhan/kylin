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

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

public class KapMergeRawTableMapper extends KylinMapper<Text, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(KapMergeRawTableMapper.class);
    protected Text outputKey = new Text();
    protected String rawTableName;
    protected RawTableInstance rawInstance;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());

        rawTableName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        rawInstance = RawTableManager.getInstance(config).getRawTableInstance(rawTableName);
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        int shardNum = rawInstance.getShardNumber() == 0 ? 10 : rawInstance.getShardNumber();
        byte[] k = key.getBytes();
        short shardId = ShardingHash.getShard(k, 0, k.length, shardNum);
        byte[] newKey = new byte[k.length + RowConstants.ROWKEY_SHARDID_LEN];
        BytesUtil.writeShort(shardId, newKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
        System.arraycopy(k, 0, newKey, RowConstants.ROWKEY_SHARDID_LEN, k.length);
        outputKey.set(newKey);
        context.write(outputKey, value);
    }
}
