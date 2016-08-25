package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.kylin.engine.mr.KylinMapper;

/**
 * Created by wangcheng on 8/25/16.
 */
public class KapMergeRawTableMapper extends KylinMapper<Text, Text, Text, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());
    }

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

    }
}
