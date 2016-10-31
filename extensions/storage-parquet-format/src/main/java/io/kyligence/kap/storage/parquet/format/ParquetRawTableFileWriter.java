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
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.BufferedRawColumnCodec;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;
import io.kyligence.kap.cube.raw.gridtable.RawTableGridTable;
import io.kyligence.kap.cube.raw.kv.RawTableConstants;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriterBuilder;

public class ParquetRawTableFileWriter extends ParquetOrderedFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileWriter.class);

    private short curShardId = 0;

    private Configuration config;
    private KylinConfig kylinConfig;
    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;
    private BufferedRawColumnCodec rawColumnsCodec;
    private String outputDir = null;

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        super.write(key, value);
    }

    public ParquetRawTableFileWriter(Path workingPath, TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {
        this.config = context.getConfiguration();
        this.tmpDir = workingPath;

        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

        outputDir = config.get(ParquetFormatConstants.KYLIN_OUTPUT_DIR);
        String rawName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        logger.info("RawTableName is " + rawName + " and segmentID is " + segmentID);
        rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(rawName);
        rawTableDesc = rawTableInstance.getRawTableDesc();
        GTInfo gtInfo = RawTableGridTable.newGTInfo(rawTableInstance);
        rawColumnsCodec = new BufferedRawColumnCodec((RawTableCodeSystem) gtInfo.getCodeSystem());

        // FIXME: Text involves array copy every time
        if (keyClass == Text.class && valueClass == Text.class) {
            logger.info("KV class is Text");
        } else {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    @Override
    protected void freshWriter(Text key, Text value) throws IOException, InterruptedException {
        byte[] keyByte = key.getBytes();
        short shardId = Bytes.toShort(keyByte, 0, RowConstants.ROWKEY_SHARDID_LEN);

        if (shardId != curShardId) {
            cleanWriter();
            curShardId = shardId;
        }

        if (writer == null) {
            writer = newWriter();
        }
    }

    @Override
    protected ParquetRawWriter newWriter() throws IOException, InterruptedException {
        ParquetRawWriter rawWriter;
        List<Type> types = new ArrayList<Type>();
        TblColRef orderedColumn = rawTableDesc.getOrderedColumn();
        List<TblColRef> columns = rawTableDesc.getColumns();

        types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, orderedColumn.getName()));
        for (TblColRef column : columns) {
            if (!column.equals(orderedColumn)) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, column.getName()));
            }
        }

        MessageType schema = new MessageType(rawTableDesc.getName(), types);
        tmpPath = new Path(tmpDir, String.valueOf(curShardId));
        rawWriter = new ParquetRawWriterBuilder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage()).setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config).setType(schema).setPath(tmpPath).build();
        return rawWriter;
    }

    @Override
    protected void writeData(Text key, Text value) {
        byte[] valueBytes = value.getBytes().clone(); //on purpose, because parquet writer will cache
        try {
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RowConstants.ROWKEY_SHARDID_LEN, key.getLength());
            int[] valueLengths = rawColumnsCodec.peekLengths(ByteBuffer.wrap(valueBytes), rawTableInstance.getRawToGridTableMapping().getNonOrderedColumnSet());
            writer.writeRow(keyBody, 0, keyBody.length, valueBytes, valueLengths);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Path getDestPath() {
        Path path = new Path(new StringBuffer().append(outputDir).append(RawTableConstants.RawTableDir).append("/").append(curShardId).append(".parquet").toString());
        return path;
    }
}
