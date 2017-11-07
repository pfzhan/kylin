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

package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IKeyEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.KeyEncodingFactory;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.IValueSetEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.ValueSetEncodingFactory;

public class ColumnIndexWriter implements Closeable {
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
        this.valueSetEncoding = ValueSetEncodingFactory.selectEncoding(columnSpec.getValueEncodingIdentifier(), 0, columnSpec.isOnlyEQIndex()); // todo: not auto calulate value encoding
        this.indexMapCache = new IndexMapCache(columnSpec.getColumnName(), !columnSpec.isOnlyEQIndex(), keyEncoding, valueSetEncoding, false);
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
        long headerSize = 0;
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexRaw.getIterable(true)) {
            Comparable key = indexEntry.getFirst();
            Iterable<? extends Number> value = indexEntry.getSecond();

            if (counter++ % step == 0) {
                keyEncoding.serialize(key, outputStream);
                outputStream.writeLong(position);
                headerSize += keyEncoding.getLength() + 8;
            }
            position += keyEncoding.getLength() + valueSetEncoding.getSerializeBytes(value);
        }

        logger.info("Offset Map for column {} built finished.", columnSpec.getColumnName());

        // write body length of bytes
        outputStream.writeLong(position);
        headerSize += 8;

        // write body bytes
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexRaw.getIterable(true)) {
            Comparable key = indexEntry.getFirst();
            Iterable<? extends Number> value = indexEntry.getSecond();
            keyEncoding.serialize(key, outputStream);
            valueSetEncoding.serialize(value, outputStream);
        }
        totalSize += headerSize + position;
        logger.info("Index block built finished. Stats: Header={}, Body={}, Step={}", headerSize, position, step);
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

        logger.info("column={}, onlyEQ={}, cardinality={}, columnLength={}, step={}, docNum={}, keyEncoding={}, valueEncoding={}", columnSpec.getColumnName(), columnSpec.isOnlyEQIndex(), indexSize, columnSpec.getColumnLength(), step, totalPageNum, keyEncoding.getEncodingIdentifier(), valueSetEncoding.getEncodingIdentifier());
        logger.info("Start to write eq index for column {}", columnSpec.getColumnName());
        writeIndex(indexMapCache, step);
        if (!columnSpec.isOnlyEQIndex()) {
            writeAuxiliary(step);
        }
    }

    private void writeAuxiliary(int step) throws IOException {
        // write lt
        logger.info("Start to write lt index for column {}", columnSpec.getColumnName());
        IndexMapCache auxiliaryIndexMap = new IndexMapCache(columnSpec.getColumnName(), false, keyEncoding, valueSetEncoding, true);
        Iterable<? extends Number> lastValue = valueSetEncoding.newValueSet();
        Iterable<? extends Number> currValue = null;
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexMapCache.getIterable(true)) {
            currValue = valueSetEncoding.or(lastValue, indexEntry.getSecond());
            auxiliaryIndexMap.putEncoded(indexEntry.getFirst(), currValue);
            lastValue = currValue;
        }
        writeIndex(auxiliaryIndexMap, step);
        auxiliaryIndexMap.close();

        // write gt
        logger.info("Start to write gt index for column {}", columnSpec.getColumnName());
        auxiliaryIndexMap = new IndexMapCache(columnSpec.getColumnName(), false, keyEncoding, valueSetEncoding, true);
        lastValue = valueSetEncoding.newValueSet();
        for (Pair<Comparable, ? extends Iterable<? extends Number>> indexEntry : indexMapCache.getIterable(false)) {
            currValue = valueSetEncoding.or(lastValue, indexEntry.getSecond());
            auxiliaryIndexMap.putEncoded(indexEntry.getFirst(), currValue);
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

    public void putNextRow(ByteArray value) {
        throw new NotImplementedException();
    }

    public void putNextRow(ByteArray[] value) {
        throw new NotImplementedException();
    }

    public void appendToRow(ByteArray value, int docId) {
        Preconditions.checkState(columnSpec.getColumnLength() == value.length());
        indexMapCache.put(value, docId);
        totalPageNum = Math.max(totalPageNum, docId + 1);
    }

    public void appendToRow(ByteArray[] values, int docId) {
        if (values != null && values.length > 0) {
            for (ByteArray value : values) {
                appendToRow(value, docId);
            }
            totalPageNum = Math.max(totalPageNum, docId + 1);
        }
    }

    public void spill() {
        indexMapCache.spill();
    }
}
