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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetBundleReader {
    public static final Logger logger = LoggerFactory.getLogger(ParquetBundleReader.class);

    List<ParquetReaderState> readerStates;

    public ParquetBundleReader(Configuration configuration, Path path, Path indexPath, ImmutableRoaringBitmap columns, ImmutableRoaringBitmap pageBitset, long fileOffset) throws IOException {
        readerStates = new ArrayList<ParquetReaderState>(columns.getCardinality());

        for (int column : columns) {
            readerStates.add(new ParquetReaderState(new ParquetColumnReaderBuilder().setFileOffset(fileOffset).setConf(configuration).setPath(path).setIndexPath(indexPath).setColumn(column).setPageBitset(pageBitset).build()));
            logger.info("Read Column: " + column);
        }
    }

    public List<Object> read() throws IOException {
        List<Object> result = new ArrayList<Object>();
        for (ParquetReaderState state : readerStates) {
            GeneralValuesReader valuesReader = state.getValuesReader();

            if (valuesReader == null) {
                return null;
            }

            Object value = valuesReader.readData();

            if (value == null) {
                if ((valuesReader = getNextValuesReader(state)) == null) {
                    return null;
                }
                value = valuesReader.readData();
                if (value == null) {
                    return null;
                }
            }

            result.add(value);
        }
        return result;
    }

    public int getPageIndex() {
        return readerStates.get(0).reader.getPageIndex();
    }

    public void close() throws IOException {
        for (ParquetReaderState state : readerStates) {
            state.reader.close();
        }
    }

    private GeneralValuesReader getNextValuesReader(ParquetReaderState state) throws IOException {
        GeneralValuesReader valuesReader = state.getNextValuesReader();
        state.setValuesReader(valuesReader);
        return valuesReader;
    }

    private class ParquetReaderState {
        private ParquetColumnReader reader;
        private GeneralValuesReader valuesReader;

        public ParquetReaderState(ParquetColumnReader reader) throws IOException {
            this.reader = reader;
            this.valuesReader = reader.getNextValuesReader();
        }

        public GeneralValuesReader getNextValuesReader() throws IOException {
            return reader.getNextValuesReader();
        }

        public GeneralValuesReader getValuesReader() {
            return valuesReader;
        }

        public void setReader(ParquetColumnReader reader) {
            this.reader = reader;
        }

        public void setValuesReader(GeneralValuesReader valuesReader) {
            this.valuesReader = valuesReader;
        }
    }
}
