package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IKeyEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.KeyEncodingFactory;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.IValueSetEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.ValueSetEncodingFactory;

public class ColumnIndexWriter implements IColumnInvertedIndex.Builder<ByteArray> {
    protected static final Logger logger = LoggerFactory.getLogger(ColumnIndexWriter.class);

    private DataOutputStream outputStream;
    private IndexMapCache indexMapCache;
    private ColumnSpec columnSpec;
    private long totalSize = -1;
    private KapConfig config;
    private IKeyEncoding keyEncoding;
    private IValueSetEncoding valueSetEncoding;
    private int totalPageNum = Integer.MIN_VALUE;

    public ColumnIndexWriter(ColumnSpec columnSpec, DataOutputStream outputStream) {
        this.outputStream = outputStream;
        this.columnSpec = columnSpec;
        this.config = KapConfig.getInstanceFromEnv();
        this.totalPageNum = columnSpec.getTotalPageNum();
        this.keyEncoding = KeyEncodingFactory.selectEncoding(columnSpec.getKeyEncodingIdentifier(), columnSpec.getColumnLength(), columnSpec.isOnlyEQIndex());
        this.valueSetEncoding = ValueSetEncodingFactory.selectEncoding(columnSpec.getValueEncodingIdentifier(), columnSpec.getCardinality(), columnSpec.isOnlyEQIndex());
        this.indexMapCache = new IndexMapCache(!columnSpec.isOnlyEQIndex(), keyEncoding, valueSetEncoding);
        logger.info("KeyEncoding={}, ValueEncoding={}", keyEncoding.getClass().getName(), valueSetEncoding.getClass().getName());
    }

    public long getTotalSize() {
        if (totalSize < 0) {
            throw new RuntimeException("cannot get totalSize before seal.");
        }
        return totalSize;
    }

    private int decideStepSize(int columnLength, int pageNum) {
        int rowBytes = columnLength + pageNum / 2 + 20; // we assume the avg size of bitmap is (pageNum / 2) + 20
        int blockBytes = 64 * 1024; // 64KB
        int step = blockBytes / rowBytes;
        step = Math.max(step, config.getParquetPageIndexStepMin());
        step = Math.min(step, config.getParquetPageIndexStepMax());

        if (step <= 0) {
            step = 1;
        }

        logger.info("Deciding step size: ColumnLength={}, PageNum={}, Step={}", columnLength, pageNum, step);
        return step;
    }

    private void writeIndex(IndexMapCache indexRaw, int step) throws IOException {
        // write index header
        int counter = 0;
        long position = 0;
        int headerSize = 0;
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexRaw.getIterable(true)) {
            Comparable key = indexEntry.getKey();
            Iterable<? extends Number> value = indexEntry.getValue();
            valueSetEncoding.runOptimize(value);

            if (counter++ % step == 0) {
                keyEncoding.serialize(key, outputStream);
                outputStream.writeLong(position);
                headerSize += keyEncoding.getLength() + 8;
            }
            position += keyEncoding.getLength() + valueSetEncoding.getSerializeBytes(value);
        }

        // write body length of bytes
        outputStream.writeLong(position);
        headerSize += 8;

        // write body bytes
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexRaw.getIterable(true)) {
            Comparable key = indexEntry.getKey();
            Iterable<? extends Number> value = indexEntry.getValue();
            keyEncoding.serialize(key, outputStream);
            valueSetEncoding.serialize(value, outputStream);
        }
        totalSize += headerSize + position;
        logger.info("Index Length Stats: Header={}, Body={}, Step={}", headerSize, position, step);
    }

    private void seal() throws IOException {
        // write metadata
        // TODO: Write ID of this index, such as magic number
        totalSize = 4 * 5 + 2 * 2;

        int step = decideStepSize(columnSpec.getColumnLength(), totalPageNum);
        int indexSize = indexMapCache.size();
        outputStream.writeInt(columnSpec.isOnlyEQIndex() ? 1 : 0);
        outputStream.writeInt(indexSize);
        outputStream.writeInt(step);
        outputStream.writeInt(columnSpec.getColumnLength());
        outputStream.writeInt(totalPageNum);

        outputStream.writeChar(keyEncoding.getEncodingIdentifier());
        outputStream.writeChar(valueSetEncoding.getEncodingIdentifier());

        logger.info("column={}, onlyEQ={}, cardinality={}, columnLength={}, step={}, docNum={}", columnSpec.getColumnName(), columnSpec.isOnlyEQIndex(), indexSize, columnSpec.getColumnLength(), step, totalPageNum);
        logger.info("Start to write eq index for column {}", columnSpec.getColumnName());
        writeIndex(indexMapCache, step);
        if (!columnSpec.isOnlyEQIndex()) {
            writeAuxiliary(step);
        }
    }

    private void writeAuxiliary(int step) throws IOException {
        // write lt
        logger.info("Start to write lt index for column {}", columnSpec.getColumnName());
        IndexMapCache auxiliaryIndexMap = new IndexMapCache(false, keyEncoding, valueSetEncoding);
        Iterable<? extends Number> lastValue = valueSetEncoding.newValueSet();
        Iterable<? extends Number> currValue = null;
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexMapCache.getIterable(true)) {
            currValue = valueSetEncoding.or(lastValue, indexEntry.getValue());
            auxiliaryIndexMap.putEncoded(indexEntry.getKey(), currValue);
            lastValue = currValue;
        }
        writeIndex(auxiliaryIndexMap, step);
        auxiliaryIndexMap.close();

        // write gt
        logger.info("Start to write gt index for column {}", columnSpec.getColumnName());
        auxiliaryIndexMap = new IndexMapCache(false, keyEncoding, valueSetEncoding);
        lastValue = valueSetEncoding.newValueSet();
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexMapCache.getIterable(false)) {
            currValue = valueSetEncoding.or(lastValue, indexEntry.getValue());
            auxiliaryIndexMap.putEncoded(indexEntry.getKey(), currValue);
            lastValue = currValue;
        }
        writeIndex(auxiliaryIndexMap, step);
        auxiliaryIndexMap.close();
    }

    @Override
    public void close() throws IOException {
        seal();
        indexMapCache.close();
        outputStream.close();
    }

    @Override
    public void putNextRow(ByteArray value) {
        throw new NotImplementedException();
    }

    @Override
    public void putNextRow(ByteArray[] value) {
        throw new NotImplementedException();
    }

    @Override
    public void appendToRow(ByteArray value, int docId) {
        Preconditions.checkState(columnSpec.getColumnLength() == value.length());
        indexMapCache.put(value, docId);
        totalPageNum = Math.max(totalPageNum, docId + 1);
    }

    @Override
    public void appendToRow(ByteArray[] values, int docId) {
        if (values != null && values.length > 0) {
            for (ByteArray value : values) {
                appendToRow(value, docId);
            }
            totalPageNum = Math.max(totalPageNum, docId + 1);
        }
    }
}
