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
    protected long counter = 0;
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
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        counter++;
        int shardNum = rawInstance.getShardNumber() == 0 ? 10 : rawInstance.getShardNumber();
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
        byte[] k = key.getBytes();
        short shardId = ShardingHash.getShard(k, 0, k.length, shardNum);
        byte[] newKey = new byte[k.length + RowConstants.ROWKEY_SHARDID_LEN];
        BytesUtil.writeShort(shardId, newKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
        System.arraycopy(k, 0, newKey, RowConstants.ROWKEY_SHARDID_LEN, k.length);
        outputKey.set(newKey);
        context.write(outputKey, value);
    }
}
