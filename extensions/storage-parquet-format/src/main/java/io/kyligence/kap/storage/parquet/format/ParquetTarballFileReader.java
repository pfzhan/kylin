package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.pageIndex.format.ParquetPageIndexRecordReader;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;

public class ParquetTarballFileReader extends RecordReader<Text, Text> {

    public enum ReadStrategy {
        KV, COMPACT
    }

    public static final Logger logger = LoggerFactory.getLogger(ParquetTarballFileReader.class);
    public static ThreadLocal<GTScanRequest> gtScanRequestThreadLocal = new ThreadLocal<>();

    protected Configuration conf;

    private ParquetBundleReader reader = null;
    private Text key = null; //key will be fixed length,
    private Text val = null; //reusing the val bytes, the returned bytes might contain useless tail, but user will use it as bytebuffer, so it's okay

    private ReadStrategy readStrategy;
    private long cuboidId;
    private short shardId;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path shardPath = fileSplit.getPath();

        long startTime = System.currentTimeMillis();
        String kylinPropsStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, "");
        if (kylinPropsStr.isEmpty()) {
            logger.warn("Creating an empty KylinConfig");
        }
        logger.info("Creating KylinConfig from conf");
        Properties kylinProps = new Properties();
        kylinProps.load(new StringReader(kylinPropsStr));
        KylinConfig.setKylinConfigInEnvIfMissing(kylinProps);

        ParquetPageIndexRecordReader indexReader = new ParquetPageIndexRecordReader();
        long fileOffset = indexReader.initialize(shardPath, context, true, true);

        String scanReqStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES);
        ImmutableRoaringBitmap pageBitmap = null;
        if (scanReqStr != null) {
            GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(scanReqStr.getBytes("ISO-8859-1")));
            gtScanRequestThreadLocal.set(gtScanRequest);//for later use convenience

            if (Boolean.valueOf(conf.get(ParquetFormatConstants.KYLIN_USE_INVERTED_INDEX))) {
                ParquetPageIndexTable indexTable = indexReader.getIndexTable();
                TupleFilter filter = gtScanRequest.getFilterPushDown();
                pageBitmap = indexTable.lookup(filter);
                logger.info("Inverted Index bitmap: " + pageBitmap + ". Time spent is: " + (System.currentTimeMillis() - startTime));
            } else {
                logger.info("Told not to use II, read all pages");
            }
        } else {
            logger.info("KYLIN_SCAN_REQUEST_BYTES not set, read all pages");
        }

        ImmutableRoaringBitmap columnBitmap = RoaringBitmaps.readFromString(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS));
        logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));

        //for readStrategy
        readStrategy = ReadStrategy.valueOf(conf.get(ParquetFormatConstants.KYLIN_TARBALL_READ_STRATEGY));
        cuboidId = Long.valueOf(shardPath.getParent().getName());
        shardId = Short.valueOf(shardPath.getName().substring(0, shardPath.getName().indexOf(".parquettar")));

        logger.info("Read Strategy is {} Cuboid id is {} and shard id is {}", readStrategy.toString(), cuboidId, shardId);

        String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
        int gtMaxLength = gtMaxLengthStr == null ? 1024 : Integer.valueOf(gtMaxLengthStr);

        val = new Text();
        val.set(new byte[gtMaxLength]);

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setFileOffset(fileOffset).setConf(conf).setPath(shardPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        if (readStrategy == ReadStrategy.KV) {
            // key
            byte[] keyBytes = ((Binary) data.get(0)).getBytes();
            if (key == null) {
                key = new Text();
                byte[] temp = new byte[keyBytes.length + RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN];//make sure length
                key.set(temp);
                Preconditions.checkState(Shorts.BYTES == RowConstants.ROWKEY_SHARDID_LEN);
                System.arraycopy(Bytes.toBytes(shardId), 0, key.getBytes(), 0, Shorts.BYTES);
                Preconditions.checkState(Longs.BYTES == RowConstants.ROWKEY_CUBOIDID_LEN);
                System.arraycopy(Bytes.toBytes(cuboidId), 0, key.getBytes(), Shorts.BYTES, Longs.BYTES);
            }
            System.arraycopy(keyBytes, 0, key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, keyBytes.length);

            //value
            setVal(data, 1);
        } else if (readStrategy == ReadStrategy.COMPACT) {
            if (key == null) {
                key = new Text();
            }
            setVal(data, 0);
        } else {
            throw new RuntimeException("unknown read strategy: " + readStrategy);
        }
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
    }
}
