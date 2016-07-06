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

public class ParquetRawRecordReader extends RecordReader<byte[], byte[]> {
    public static final Logger logger = LoggerFactory.getLogger(ParquetRawRecordReader.class);

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
        KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(conf.get(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES)));
        ParquetPageIndexRecordReader indexReader = new ParquetPageIndexRecordReader();
        long fileOffset = indexReader.initialize(split, context);
        ParquetPageIndexTable indexTable = indexReader.getIndexTable();
        GTScanRequest gtScanRequest = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST_BYTES).getBytes("ISO-8859-1")));
        gtScanRequestThreadLocal.set(gtScanRequest);
        TupleFilter filter = gtScanRequest.getFilterPushDown();
        ImmutableRoaringBitmap pageBitmap = indexTable.lookup(null);
        logger.info("Inverted Index bitmap: " + pageBitmap + ". Time spent is: " + (System.currentTimeMillis() - startTime));

        ImmutableRoaringBitmap measureBitmap = readBitmap(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP);
        MutableRoaringBitmap columnBitmap = MutableRoaringBitmap.bitmapOf(0);
        for (int i : measureBitmap) {
            columnBitmap.add(i + 1);
        }
        logger.info("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));

        int gtMaxLength = Integer.valueOf(conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH));
        val = new byte[gtMaxLength];

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setFileOffset(fileOffset).setConf(conf).setPath(shardPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
    }

    private ImmutableRoaringBitmap readBitmap(String property) throws IOException {
        String pageBitsetString = conf.get(property);
        ImmutableRoaringBitmap bitmap = null;

        if (pageBitsetString != null) {
            ByteBuffer buf = ByteBuffer.wrap(pageBitsetString.getBytes("ISO-8859-1"));
            bitmap = new ImmutableRoaringBitmap(buf);
        }
        return bitmap;
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
        int offset = 0;
        for (int i = 0; i < data.size(); ++i) {
            byte[] src = ((Binary) data.get(i)).getBytes();
            System.arraycopy(src, 0, val, offset, src.length);
            offset += src.length;
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
