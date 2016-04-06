/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package io.kyligence.kap.engine.mr;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.BaseCuboidMapperBase;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class SecondaryIndexMapper<KEYIN> extends BaseCuboidMapperBase<KEYIN, Object> {

    private IMRTableInputFormat flatTableInputFormat;
    private RowKeyColumnIO colIO;
    private ByteBuffer keyBuffer;
    private int colNeedIndex;
    private int headerLength;
    private int indexColTotalLen;

    public static int COLUMN_ID_LENGTH = 1;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
        colIO = new RowKeyColumnIO(new CubeDimEncMap(cubeSegment));
        keyBuffer = ByteBuffer.allocate(4096);
        colNeedIndex = cubeSegment.getCubeDesc().getRowkey().getRowKeyColumns().length; //FIXME: read from metadata
        headerLength = ((RowKeyEncoder)rowKeyEncoder).getHeaderLength();

        for (int i = 0; i < colNeedIndex; i++) {
            TblColRef column = baseCuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(column);

            if (colLength > Bytes.SIZEOF_INT) {
                throw new IllegalStateException("Column " + i + " encoding length > 4, is not suitable for secondary index.");
            }
            indexColTotalLen += colLength;
        }
    }

    @Override
    public void map(KEYIN key, Object value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        keyBuffer.clear();
        try {
            //put a record into the shared bytesSplitter
            String[] row = flatTableInputFormat.parseMapperInput(value);
            bytesSplitter.setBuffers(convertUTF8Bytes(row));

            byte[] rowkey = buildKey(bytesSplitter.getSplitBuffers());

            for (int i = 0; i < colNeedIndex; i++) {
                int position = keyBuffer.position();
                BytesUtil.writeUnsigned(i, COLUMN_ID_LENGTH, keyBuffer);
                keyBuffer.put(rowkey, headerLength, indexColTotalLen);
                outputKey.set(keyBuffer.array(), position, COLUMN_ID_LENGTH + indexColTotalLen);
                context.write(outputKey, outputValue);
            }

        } catch (Exception ex) {
            handleErrorRecord(bytesSplitter, ex);
        }
    }
}
