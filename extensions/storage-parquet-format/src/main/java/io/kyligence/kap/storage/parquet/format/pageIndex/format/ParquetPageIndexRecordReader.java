package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import org.apache.commons.io.IOUtils;
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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.gridtable.GTScanRequest;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;

public class ParquetPageIndexRecordReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private GTScanRequest scanRequest;
    private Path shardPath;
    private ParquetPageIndexTable indexTable = null;
    private FSDataInputStream inputStream = null;

    private K key;
    private V val;
    private boolean hasRead = false;
    private TupleFilter filter = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(""));

        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

//        scanRequest = GTScanRequest.serializer.deserialize(new ByteArray(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST).getBytes()).asBuffer());
        filter = mockupFilterForTest();
        inputStream = FileSystem.get(conf).open(shardPath);
        indexTable = new ParquetPageIndexTable(inputStream);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (hasRead) {
            key = null;
            val = null;
            return false;
        } else {
            key = (K) new Text(shardPath.toString());
            val = (V) new SerializableImmutableRoaringBitmap(indexTable.lookup(filter));
            hasRead = true;
            return true;
        }
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        indexTable.close();
    }

    private TupleFilter mockupFilterForTest() {
        int dataSize = 50;
        int columnLength = Integer.SIZE - Integer.numberOfLeadingZeros(dataSize);
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", null).getRef()));
        byte[] buffer = new byte[columnLength];
        BytesUtil.writeUnsigned(10, buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        return filter;
    }
}
