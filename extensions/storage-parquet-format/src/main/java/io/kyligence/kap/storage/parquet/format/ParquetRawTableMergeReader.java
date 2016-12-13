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

package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;

public class ParquetRawTableMergeReader extends RecordReader<Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetRawTableMergeReader.class);

    protected Configuration conf;

    private Path shardPath;
    private ParquetBundleReader reader = null;

    private Text key = null; //key will be fixed length,
    private Text val = null; //reusing the val bytes, the returned bytes might contain useless tail, but user will use it as bytebuffer, so it's okay
    private KylinConfig kylinConfig;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        shardPath = fileSplit.getPath();

        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

        logger.info("********************shard file path: {}", shardPath.toString());

        String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
        int gtMaxLength = gtMaxLengthStr == null ? 1024 : Integer.valueOf(gtMaxLengthStr);

        val = new Text();
        val.set(new byte[gtMaxLength]);

        // init with first shard file

        reader = new ParquetBundleReader.Builder().setConf(conf).setPath(shardPath).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> row = reader.read();

        if (row == null) {
            return false;
        }

        if (null == key) {
            key = new Text();
        }
        byte[] keyValue = ((Binary) row.get(0)).getBytes().clone();
        key.set(keyValue);
        setVal(row, 1);
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private void setVal(List<Object> data, int start) {
        int retry = 0;
        while (true) {
            try {
                int offset = 0;
                for (int i = start; i < data.size(); ++i) {
                    byte[] src = ((Binary) data.get(i)).getBytes();
                    System.arraycopy(src, 0, val.getBytes(), offset, src.length);
                    offset += src.length;
                }
                break;
            } catch (ArrayIndexOutOfBoundsException e) {
                if (++retry > 10) {
                    throw new IllegalStateException("RawTable columns taking too much space! ");
                }
                byte[] temp = new byte[val.getBytes().length * 2];
                val.set(temp);
                logger.info("val buffer size adjusted to: " + val.getBytes().length);
            }
        }
    }
}
