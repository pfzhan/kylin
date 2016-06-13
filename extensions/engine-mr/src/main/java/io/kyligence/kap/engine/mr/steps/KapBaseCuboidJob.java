package io.kyligence.kap.engine.mr.steps;

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.CuboidJob;
import org.apache.kylin.engine.mr.steps.HiveToBaseCuboidMapper;

public class KapBaseCuboidJob extends KapCuboidJob {
    public KapBaseCuboidJob() {
        this.setMapperClass(HiveToBaseCuboidMapper.class);
    }

    public static void main(String[] args) throws Exception {
        CuboidJob job = new BaseCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
