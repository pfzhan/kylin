package io.kyligence.kap.engine.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.storage.StorageFactory;

public class KapMRUtil {
    public static IMRInput.IMRBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        return SourceFactory.createEngineAdapter(seg, IMRInput.class).getBatchCubingInputSide(seg);
    }

    public static IMRInput.IMRTableInputFormat getTableInputFormat(String tableName) {
        return getTableInputFormat(getTableDesc(tableName));
    }

    public static IMRInput.IMRTableInputFormat getTableInputFormat(TableDesc tableDesc) {
        return SourceFactory.createEngineAdapter(tableDesc, IMRInput.class).getTableInputFormat(tableDesc);
    }

    private static TableDesc getTableDesc(String tableName) {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(tableName);
    }

    public static IMROutput.IMRBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput.class).getBatchCubingOutputSide(seg);
    }

    public static IMROutput.IMRBatchMergeOutputSide getBatchMergeOutputSide(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput.class).getBatchMergeOutputSide(seg);
    }

    public static IMROutput2.IMRBatchCubingOutputSide2 getBatchCubingOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchCubingOutputSide(seg);
    }

    public static IMROutput2.IMRBatchMergeOutputSide2 getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchMergeOutputSide(seg);
    }

    public static IMROutput3.IMRBatchCubingOutputSide3 getBatchCubingOutputSide3(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput3.class).getBatchCubingOutputSide(seg);
    }

    // use this method instead of ToolRunner.run() because ToolRunner.run() is not thread-sale
    // Refer to: http://stackoverflow.com/questions/22462665/is-hadoops-toorunner-thread-safe
    public static int runMRJob(Tool tool, String[] args) throws Exception {
        Configuration conf = tool.getConf();
        if (conf == null) {
            conf = new Configuration();
        }

        GenericOptionsParser parser = getParser(conf, args);
        //set the configuration back, so that Tool can configure itself
        tool.setConf(conf);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();
        return tool.run(toolArgs);
    }

    private static synchronized GenericOptionsParser getParser(Configuration conf, String[] args) throws Exception {
        return new GenericOptionsParser(conf, args);
    }
}
