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

package io.kyligence.kap.engine.mr.index;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
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

/**
 */
public class SecondaryIndexMapper<KEYIN> extends BaseCuboidMapperBase<KEYIN, Object> {

    private IMRTableInputFormat flatTableInputFormat;
    private RowKeyColumnIO colIO;
    private ByteBuffer keyBuffer;
    private int[] colNeedIndex;
    private int headerLength;
    private int indexColTotalLen;

    private int lastIndexCol = 0;

    public static final int COLUMN_ID_LENGTH = 1;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
        colIO = new RowKeyColumnIO(new CubeDimEncMap(cubeSegment));
        keyBuffer = ByteBuffer.allocate(1024);

        colNeedIndex = cubeSegment.getCubeDesc().getRowkey().getColumnsNeedIndex();
        lastIndexCol = colNeedIndex[colNeedIndex.length - 1]; // last index col
        headerLength = ((RowKeyEncoder) rowKeyEncoder).getHeaderLength();

        for (int i = 0; i <= lastIndexCol; i++) {
            TblColRef column = baseCuboid.getColumns().get(i);
            int colLength = colIO.getColumnLength(column);
            indexColTotalLen += colLength;

            if (ArrayUtils.contains(colNeedIndex, i) && colLength > Bytes.SIZEOF_INT) {
                throw new IllegalStateException("Column " + column + " encoding length > 4, is not suitable for secondary index.");
            }

        }
    }

    @Override
    public void doMap(KEYIN key, Object value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        keyBuffer.clear();
        try {
            String[] row = flatTableInputFormat.parseMapperInput(value);
            bytesSplitter.setBuffers(convertUTF8Bytes(row));

            byte[] rowkey = buildKey(bytesSplitter.getSplitBuffers());

            for (int i = 0; i < colNeedIndex.length; i++) {
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
