package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;

import io.kyligence.kap.storage.parquet.format.file.GeneralValuesReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetColumnReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetColumnReaderBuilder;

public class ParquetPageRecordReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private Path shardPath;
    private ParquetColumnReader reader = null;
    private GeneralValuesReader valuesReader = null;

    private K key;
    private V val;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

        // init with first shard file
        reader = new ParquetColumnReaderBuilder().setConf(conf).setPath(shardPath).build();
        valuesReader = reader.getNextValuesReader();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Binary keyBytes = valuesReader.readBytes();
        if (keyBytes == null) {
            valuesReader = reader.getNextValuesReader();
            if (valuesReader == null) {
                return false;
            }
            keyBytes = valuesReader.readBytes();
            if (keyBytes == null) {
                return false;
            }
        }

        key = (K) new Text(keyBytes.getBytes());
        val = (V) new IntWritable(reader.getPageIndex());
        return true;
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
        reader.close();
    }
}
