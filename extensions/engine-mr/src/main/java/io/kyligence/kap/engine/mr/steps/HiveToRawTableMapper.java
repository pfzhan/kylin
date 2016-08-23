package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;

public class HiveToRawTableMapper<KEYIN> extends RawTableMapperBase<KEYIN, Object> {
    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();

    }

    @Override
    public void map(KEYIN key, Object value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        try {
            //put a record into the shared bytesSplitter
            String[] row = flatTableInputFormat.parseMapperInput(value);
            bytesSplitter.setBuffers(convertUTF8Bytes(row));
            //take care of the data in bytesSplitter
            outputKV(context);

        } catch (Exception ex) {
            handleErrorRecord(bytesSplitter, ex);
        }
    }

}
