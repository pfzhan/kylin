/**
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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.filter.TupleFilterSerializerExt;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetOrderedPageIndexTable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;
import io.kyligence.kap.storage.parquet.format.serialize.TupleFilterLiteralHasher;

public class ParquetRawTableFileReader extends RecordReader<Text, Text> {

    public enum ReadStrategy {
        KV, COMPACT
    }

    public static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileReader.class);
    public static ThreadLocal<GTScanRequest> gtScanRequestThreadLocal = new ThreadLocal<>();

    protected Configuration conf;

    private ParquetBundleReader reader = null;
    ParquetPageIndexTable indexTable = null;
    private Text key = null; //key will be fixed length,
    private Text val = null; //reusing the val bytes, the returned bytes might contain useless tail, but user will use it as bytebuffer, so it's okay

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path parquetPath = fileSplit.getPath();
        Path indexPath = new Path(parquetPath.toString() + ".inv");

        long startTime = System.currentTimeMillis();
        String kylinPropsStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, "");
        if (kylinPropsStr.isEmpty()) {
            logger.warn("Creating an empty KylinConfig");
        }
        logger.info("Creating KylinConfig from conf");
        Properties kylinProps = new Properties();
        kylinProps.load(new StringReader(kylinPropsStr));
        KylinConfig.setKylinConfigInEnvIfMissing(kylinProps);

        // read index file 
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(indexPath);
        indexTable = new ParquetOrderedPageIndexTable(fileSystem, indexPath, inputStream, 0, Collections.singleton(0));

        // page bitmap
        String scanReqStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES);
        ImmutableRoaringBitmap pageBitmap = null;
        if (scanReqStr != null) {
            GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(scanReqStr.getBytes("ISO-8859-1")));
            gtScanRequestThreadLocal.set(gtScanRequest);//for later use convenience

            if (Boolean.valueOf(conf.get(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX))) {
                TupleFilter filter = gtScanRequest.getFilterPushDown();

                //for Rawtable filters, replace all the literals with hash value first
                TupleFilterLiteralHasher decorator = new TupleFilterLiteralHasher();
                IFilterCodeSystem<ByteArray> wrap = GTUtil.wrap(gtScanRequest.getInfo().getCodeSystem().getComparator());
                byte[] serialize = TupleFilterSerializerExt.serialize(filter, decorator, wrap);
                TupleFilter hashedFilter = TupleFilterSerializerExt.deserialize(serialize, wrap);

                logger.info("Starting to lookup inverted index");
                pageBitmap = indexTable.lookup(hashedFilter);
                logger.info("Inverted Index bitmap: " + pageBitmap + ". Time spent is: " + (System.currentTimeMillis() - startTime));
            } else {
                logger.info("Told not to use II, read all pages");
            }
        } else {
            logger.info("KYLIN_SCAN_REQUEST_BYTES not set, read all pages");
        }

        // column bitmap
        ImmutableRoaringBitmap columnBitmap = RoaringBitmaps.readFromString(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS));
        if (columnBitmap != null) {
            logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));
        } else {
            logger.info("All columns read by parquet is not set");
        }

        String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
        int gtMaxLength = gtMaxLengthStr == null ? 1024 : Integer.valueOf(gtMaxLengthStr);

        val = new Text();
        val.set(new byte[gtMaxLength]);

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setFileOffset(0).setConf(conf).setPath(parquetPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        setVal(data, 0);
        return true;
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
                    throw new IllegalStateException("Measures taking too much space! ");
                }
                byte[] temp = new byte[val.getBytes().length * 2];
                val.set(temp);
                logger.info("val buffer size adjusted to: " + val.getBytes().length);
            }
        }
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
        indexTable.close();
    }
}
