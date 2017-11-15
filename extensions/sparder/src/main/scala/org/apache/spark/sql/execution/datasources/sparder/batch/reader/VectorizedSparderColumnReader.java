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

package org.apache.spark.sql.execution.datasources.sparder.batch.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.kyligence.kap.storage.parquet.format.file.ParquetMetrics;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import io.kyligence.kap.common.util.ExpandableBytesVector;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawReader;
import io.kyligence.kap.storage.parquet.format.file.pagereader.PageValuesReader;

public class VectorizedSparderColumnReader {
    private static ParquetMetadataConverter converter = new ParquetMetadataConverter();

    private PageReader pageReader;
    private ColumnDescriptor columnDescriptor;
    private ExpandableBytesVector buffer;

    public VectorizedSparderColumnReader(PageReader pageReader, ColumnDescriptor columnDescriptor) {
        this.pageReader = pageReader;
        this.columnDescriptor = columnDescriptor;
        buffer = new ExpandableBytesVector(10000);
    }

    public void reset(PageReader pageReader) {
        this.pageReader = pageReader;
    }

    public ExpandableBytesVector readNextPage() {
        DataPage page = pageReader.readPage();
        if (page == null) {
            return null;
        }
        // TODO: Why is this a visitor?
        page.accept(new DataPage.Visitor<Void>() {
            @Override
            public Void visit(DataPageV1 dataPageV1) {
                try {
                    readPageV1(dataPageV1);
                    return null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Void visit(DataPageV2 dataPageV2) {
                try {
                    readPageV2(dataPageV2);
                    return null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return buffer;
    }

    private void readPageV1(DataPageV1 page) throws IOException {
        ParquetMetrics.get().bufferReadStart();
        org.apache.parquet.format.Encoding rlEncoding = converter.getEncoding(page.getRlEncoding());
        org.apache.parquet.format.Encoding dlEncoding = converter.getEncoding(page.getDlEncoding());
        int valueCount = page.getValueCount();
        int offset = skipLevels(valueCount, columnDescriptor, rlEncoding, dlEncoding, page.getBytes().toByteArray(), 0);
        PageValuesReader pageReader = ParquetRawReader.getPageValuesReader(
                converter.getEncoding(page.getValueEncoding()), columnDescriptor, ValuesType.VALUES);
        pageReader.initFromPage(valueCount, page.getBytes().toByteBuffer(), offset);
        buffer.setTotalLength(0);
        buffer.setRowCount(0);
        pageReader.readPage(buffer);
        ParquetMetrics.get().bufferReadEnd();

    }

    private int skipLevels(int numValues, ColumnDescriptor descriptor, org.apache.parquet.format.Encoding rEncoding,
            org.apache.parquet.format.Encoding dEncoding, byte[] in, int offset) throws IOException {
        offset = skipRepetitionLevel(numValues, descriptor, rEncoding, in, offset).getNextOffset();
        offset = skipDefinitionLevel(numValues, descriptor, dEncoding, in, offset).getNextOffset();
        return offset;
    }

    private ValuesReader skipRepetitionLevel(int numValues, ColumnDescriptor descriptor,
            org.apache.parquet.format.Encoding encoding, byte[] in, int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.REPETITION_LEVEL, in, offset);
    }

    private ValuesReader skipDefinitionLevel(int numValues, ColumnDescriptor descriptor,
            org.apache.parquet.format.Encoding encoding, byte[] in, int offset) throws IOException {
        return skipLevel(numValues, descriptor, encoding, ValuesType.DEFINITION_LEVEL, in, offset);
    }

    private ValuesReader skipLevel(int numValues, ColumnDescriptor descriptor,
            org.apache.parquet.format.Encoding encoding, ValuesType type, byte[] in, int offset) throws IOException {
        ValuesReader reader = ParquetRawReader.getValuesReader(encoding, descriptor, type);
        reader.initFromPage(numValues, ByteBuffer.wrap(in), offset);
        return reader;
    }

    private void readPageV2(DataPageV2 page) throws IOException {
        ParquetMetrics.get().bufferReadStart();
        int rowCount = page.getRowCount();
        PageValuesReader pageReader = ParquetRawReader.getPageValuesReader(
                converter.getEncoding(page.getDataEncoding()), columnDescriptor, ValuesType.VALUES);
        pageReader.initFromPage(rowCount, page.getData().toByteBuffer(), 0);
        buffer.setTotalLength(0);
        buffer.setRowCount(0);
        pageReader.readPage(buffer);
        ParquetMetrics.get().bufferReadEnd();
    }
}
