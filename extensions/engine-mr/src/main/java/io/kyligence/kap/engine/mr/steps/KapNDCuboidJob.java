package io.kyligence.kap.engine.mr.steps;

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.engine.mr.steps.NDCuboidMapper;

public class KapNDCuboidJob extends KapCuboidJob {

    public KapNDCuboidJob() {
        this.setMapperClass(NDCuboidMapper.class);
    }

    public static void main(String[] args) throws Exception {
        KapCuboidJob job = new KapNDCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
