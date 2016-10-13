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

package io.kyligence.kap.cube.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DateStrDictionary;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnInvertedIndexWriter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ColumnInvertedIndexWriter.class);

    private int colOffset = 0;
    private int colLength;
    private IColumnInvertedIndex.Builder invertedIndexBuilder;

    /**
     * Only for UT
     *
     * @param colName
     * @param cardinality
     * @param colLength
     * @param ivdIdx
     */
    public ColumnInvertedIndexWriter(String colName, int cardinality, int colLength, File ivdIdx) {
        this.colLength = colLength;
        invertedIndexBuilder = ColumnIndexFactory.createLocalInvertedIndex(colName, cardinality, ivdIdx.getAbsolutePath()).rebuild();
    }

    /**
     * Constructor for real usage. Will check the dictionary type and use a proper cardinality
     *
     * @param col
     * @param dictionary
     * @param length
     * @param ivdIdx
     */
    public ColumnInvertedIndexWriter(TblColRef col, Dictionary<String> dictionary, int length, File ivdIdx) {
        this.colLength = length;
        int cardinality = dictionary.getSize();
        if (dictionary instanceof DateStrDictionary) {
            cardinality = cardinality / 4; // 0000 to 2500 year
        } else if (!(dictionary instanceof TrieDictionary)) {
            throw new IllegalArgumentException("Not support to build secondary dictionary for col " + col);
        }

        logger.info("Build inverted index for col " + col + ", cardinality is " + cardinality);
        if (cardinality > 1000000) {
            logger.warn("Ultra high cardinality column, may eat much memory.");
        }

        invertedIndexBuilder = ColumnIndexFactory.createLocalInvertedIndex(col.getName(), cardinality, ivdIdx.getAbsolutePath()).rebuild();
    }

    public void write(byte[] bytes, int rowId) throws IOException {
        write(bytes, 0, bytes.length, rowId);
    }

    public void write(byte[] bytes, int offset, int length, int rowId) throws IOException {
        assert length >= colLength && colOffset + offset + colLength <= bytes.length;

        int value = BytesUtil.readUnsigned(bytes, colOffset + offset, colLength);
        //write the value to the index files
        invertedIndexBuilder.appendToRow(value, rowId);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(invertedIndexBuilder);
    }
}
