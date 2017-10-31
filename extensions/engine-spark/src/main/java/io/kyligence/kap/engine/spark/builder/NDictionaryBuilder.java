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

package io.kyligence.kap.engine.spark.builder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;

public class NDictionaryBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(NDictionaryBuilder.class);

    private static final long DICTIONARY_OOM_THRESHOLD = 15000000;
    private Dataset<Row> dataSet;
    private NDataSegment seg;

    public NDictionaryBuilder(NDataSegment seg, Dataset<Row> dataSet) {
        this.seg = seg;
        this.dataSet = dataSet;
    }

    public void buildDictionary() throws Exception {

        final NDataflow dataflow = seg.getDataflow();
        final NCubePlan cubePlan = dataflow.getCubePlan();

        final long start = System.currentTimeMillis();
        Map<TblColRef, Dictionary<String>> dictionaryMap = Maps.newHashMap();

        for (TblColRef col : cubePlan.getAllColumnsHaveDictionary()) {
            Dictionary<String> existing = seg.getDictionary(col);
            if (existing != null)
                continue;
            int id = cubePlan.getModel().getColumnIdByColumnName(col.getIdentity());
            final Dataset<Row> afterDistinct = dataSet.select(String.valueOf(id)).distinct();

            long count = afterDistinct.count();

            logger.info("Building dictionary for column: {}, its cardinality is: {}", col.getIdentity(), count);

            if (count > DICTIONARY_OOM_THRESHOLD) {
                logger.info(
                        "Warning: It's very risky to build the dictionary for column: {}, while its \n"
                                + "cardinality={} is very high, might cause out of memory exception.",
                        col.getIdentity(), count);
            }

            final List<Row> rows = afterDistinct.collectAsList();
            dictionaryMap.put(col, DictionaryGenerator.buildDictionary(col.getType(),
                    new IterableDictionaryValueEnumerator(new Iterable<String>() {
                        @Override
                        public Iterator<String> iterator() {
                            return new Iterator<String>() {
                                int i = 0;

                                @Override
                                public boolean hasNext() {
                                    return i < rows.size();
                                }

                                @Override
                                public String next() {
                                    if (hasNext()) {
                                        final Row row = rows.get(i++);
                                        final Object o = row.get(0);
                                        return o != null ? o.toString() : null;
                                    } else {
                                        throw new NoSuchElementException();
                                    }
                                }

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        }
                    })));
        }
        final long end = System.currentTimeMillis();
        writeDictionary(seg, dictionaryMap, start, end);
        try {
            NDataflowUpdate nBuilder = new NDataflowUpdate(dataflow);
            nBuilder.setToUpdateSegs(seg);
            NDataflowManager.getInstance(seg.getConfig()).updateDataflow(nBuilder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    private Map<TblColRef, Dictionary<String>> writeDictionary(NDataSegment segment,
            Map<TblColRef, Dictionary<String>> dictionaryMap, long startOffset, long endOffset) {
        Map<TblColRef, Dictionary<String>> realDictMap = Maps.newHashMap();

        for (Map.Entry<TblColRef, Dictionary<String>> entry : dictionaryMap.entrySet()) {
            final TblColRef tblColRef = entry.getKey();
            final Dictionary<String> dictionary = entry.getValue();
            IReadableTable.TableSignature signature = new IReadableTable.TableSignature();
            signature.setLastModifiedTime(System.currentTimeMillis());
            signature.setPath(String.format("streaming_%s_%s", startOffset, endOffset));
            signature.setSize(endOffset - startOffset);
            DictionaryInfo dictInfo = new DictionaryInfo(tblColRef.getColumnDesc(), tblColRef.getDatatype(), signature);
            logger.info("writing dictionary for TblColRef:" + tblColRef.toString());
            DictionaryManager dictionaryManager = DictionaryManager.getInstance(segment.getConfig());
            try {
                DictionaryInfo realDict = dictionaryManager.trySaveNewDict(dictionary, dictInfo);
                segment.putDictResPath(tblColRef, realDict.getResourcePath());
                realDictMap.put(tblColRef, realDict.getDictionaryObject());
            } catch (IOException e) {
                throw new RuntimeException("error save dictionary for column:" + tblColRef, e);
            }
        }
        return realDictMap;
    }
}
