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

import java.io.IOException;
import java.util.Collection;

import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveToRawTableMapper<KEYIN> extends RawTableMapperBase<KEYIN, Object> {
    protected static final Logger logger = LoggerFactory.getLogger(HiveToRawTableMapper.class);
    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.doSetup(context);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
    }

    @Override
    public void doMap(KEYIN key, Object value, Context context) throws IOException, InterruptedException {
        Collection<String[]> rowCollection = flatTableInputFormat.parseMapperInput(value);

        for (String[] row: rowCollection) {
            //put a record into the shared bytesSplitter
            try {
                // If split size is not enough
                if (row.length > bytesSplitter.getSplitBuffers().length) {
                    logger.info("byteSplitter is not enough, expand to {} columns", row.length);
                    bytesSplitter = new BytesSplitter(row.length, bytesSplitter.getSplitBuffers()[0].length);
                }
                bytesSplitter.setBuffers(convertUTF8Bytes(row));
                //take care of the data in bytesSplitter
                outputKV(context);
            } catch (Exception ex) {
                handleErrorRecord(bytesSplitter, ex);
            }
        }
    }

}
