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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;

public class KapMergeRawTableMapper extends KylinMapper<ByteArrayListWritable, ByteArrayListWritable, ByteArrayListWritable, ByteArrayListWritable> {

    protected static final Logger logger = LoggerFactory.getLogger(KapMergeRawTableMapper.class);
    protected String rawTableName;
    protected RawTableInstance rawInstance;
    protected RawTableDesc rawDesc;
    private ImmutableBitSet shardbyBitmap;
    private int sortbyCnt;
    private int allColumnCnt;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());

        rawTableName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        rawInstance = RawTableManager.getInstance(config).getRawTableInstance(rawTableName);
        rawDesc = rawInstance.getRawTableDesc();
        shardbyBitmap = rawDesc.getRawToGridTableMapping().getShardbyKey();
        sortbyCnt = rawDesc.getSortbyColumns().size();
        allColumnCnt = rawDesc.getColumnsInOrder().size();
    }

    @Override
    public void doMap(ByteArrayListWritable key, ByteArrayListWritable value, Context context) throws IOException, InterruptedException {
        int shardNum = rawInstance.getShardNumber() == 0 ? 10 : rawInstance.getShardNumber();
        short shardId = getShardId(key, value, shardNum);
        ByteArrayListWritable newKey = new ByteArrayListWritable(new ArrayList<>(key.get().subList(0, sortbyCnt)));
        ByteArrayListWritable newVal = new ByteArrayListWritable(key.get().subList(sortbyCnt, allColumnCnt));
        newKey.add(BytesUtil.writeShort(shardId));
        context.write(newKey, newVal);
    }

    protected short getShardId(ByteArrayListWritable key, ByteArrayListWritable value, int shardNum) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int index : shardbyBitmap) {
            baos.write(key.get().get(index));
        }
        baos.close();
        byte[] shardBytes = baos.toByteArray();
        return ShardingHash.getShard(shardBytes, 0, shardBytes.length, shardNum);
    }
}
