package io.kyligence.kap.engine.mr.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.steps.MapContextGTRecordWriter;

import java.io.IOException;

/**
 * Created by roger on 5/26/16.
 */
public class KapMapContextGTRecordWriter extends KapKVGTRecordWriter {

    private static final Log logger = LogFactory.getLog(MapContextGTRecordWriter.class);
    protected MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext;

    public KapMapContextGTRecordWriter(MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext, CubeDesc cubeDesc, CubeSegment cubeSegment) {
        super(cubeDesc, cubeSegment);
        this.mapContext = mapContext;
    }

    @Override
    protected void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException {
        try {
            mapContext.write(key, value);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

}
