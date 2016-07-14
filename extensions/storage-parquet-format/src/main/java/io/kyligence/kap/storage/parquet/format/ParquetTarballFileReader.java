package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import io.kyligence.kap.storage.parquet.format.pageIndex.format.ParquetPageIndexRecordReader;
import io.kyligence.kap.storage.parquet.format.serialize.RoaringBitmaps;

public class ParquetTarballFileReader extends RecordReader<byte[], byte[]> {
    public static final Logger logger = LoggerFactory.getLogger(ParquetTarballFileReader.class);

    public static ThreadLocal<GTScanRequest> gtScanRequestThreadLocal = new ThreadLocal<>();

    protected Configuration conf;

    private Path shardPath;
    private ParquetBundleReader reader = null;

    private static byte[] key = new byte[0];
    private byte[] val = null;//reusing the val bytes, the returned bytes might contain useless tail

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        shardPath = fileSplit.getPath();

        long startTime = System.currentTimeMillis();
        String kylinPropertiesStr = conf.get(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES);
        if (kylinPropertiesStr == null) {
            KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(""));
        } else {
            KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(kylinPropertiesStr));
        }
        ParquetPageIndexRecordReader indexReader = new ParquetPageIndexRecordReader();
        long fileOffset = indexReader.initialize(split, context);

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

        ImmutableRoaringBitmap measureBitmap = RoaringBitmaps.readFromString(conf.get(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP));
        MutableRoaringBitmap columnBitmap = null;
        if (measureBitmap != null) {
            columnBitmap = MutableRoaringBitmap.bitmapOf(0);
            for (int i : measureBitmap) {
                columnBitmap.add(i + 1);//dimensions occupying the first column in parquet, so measures' index should inc one
            }
        }
        logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));

        String gtMaxLengthStr = conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH);
        int gtMaxLength = gtMaxLengthStr == null ? 1024 : Integer.valueOf(gtMaxLengthStr);
        val = new byte[gtMaxLength];

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setFileOffset(fileOffset).setConf(conf).setPath(shardPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        setVal(data);
        return true;
    }

    // We put all columns in values and keep key empty
    private void setVal(List<Object> data) {
        int retry = 0;
        while (true) {
            try {
                int offset = 0;
                for (int i = 0; i < data.size(); ++i) {
                    byte[] src = ((Binary) data.get(i)).getBytes();
                    System.arraycopy(src, 0, val, offset, src.length);
                    offset += src.length;
                }
                break;
            } catch (ArrayIndexOutOfBoundsException e) {
                if (++retry > 10) {
                    throw new IllegalStateException("Measures taking too much space! ");
                }
                val = new byte[val.length * 2];
                logger.info("val buffer size adjusted to: " + val.length);
            }
        }
    }

    @Override
    public byte[] getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public byte[] getCurrentValue() throws IOException, InterruptedException {
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
