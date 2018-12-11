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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.INDictionaryBuilder;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.dict.NDictionaryInfo;
import org.apache.kylin.dict.NDictionaryManager;
import org.apache.kylin.dict.NGlobalDictionaryBuilder2;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import scala.Tuple2;

public class NDictionaryBuilder implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(NDictionaryBuilder.class);
    private Dataset<Row> dataSet;
    private NDataSegment seg;

    public NDictionaryBuilder(NDataSegment seg, Dataset<Row> dataSet) {
        this.seg = seg;
        this.dataSet = dataSet;
    }

    public NDataSegment buildDictionary() throws Exception {

        logger.info("building dictionaries for seg {}", seg);

        final NDataflow dataflow = seg.getDataflow();
        final NCubePlan cubePlan = dataflow.getCubePlan();

        final long start = System.currentTimeMillis();
        Map<TblColRef, Dictionary<String>> dictionaryMap = Maps.newHashMap();

        for (TblColRef col : cubePlan.getAllColumnsNeedDictionaryBuilt()) {
            NDictionaryInfo dictInfo = new NDictionaryInfo(col.getColumnDesc(), col.getDatatype(), null,
                    seg.getProject());
            String dictionaryBuilderClass = cubePlan.getDictionaryBuilderClass(col);
            //TODO: what if dict changed?
            Dictionary<String> existing = seg.getDictionary(col);
            if (existing != null)
                continue;
            int id = cubePlan.getModel().getColumnIdByColumnName(col.getIdentity());
            final Dataset<Row> afterDistinct = dataSet.select(String.valueOf(id)).distinct();

            final List<String> rows = new ArrayList<>();

            if (dictionaryBuilderClass != null && isUsingGlobalDict2(dictionaryBuilderClass)) {
                int partitions = seg.getConfig().getAppendDictHashPartitions();
                final Collection<String> ret = afterDistinct.toJavaRDD()
                        .mapToPair(new PairFunction<Row, String, String>() {
                            @Override
                            public Tuple2<String, String> call(Row row) throws Exception {
                                if (row.get(0) == null)
                                    return new Tuple2<>(null, null);

                                return new Tuple2<>(row.get(0).toString(), row.get(0).toString());
                            }
                        }).partitionBy(new NHashPartitioner(partitions)).collectAsMap().values();
                rows.addAll(ret);
            } else {
                final List<String> ret = afterDistinct.toJavaRDD().map(new Function<Row, String>() {
                    @Override
                    public String call(Row value) throws Exception {
                        return value.get(0) == null ? null : value.get(0).toString();
                    }
                }).collect();
                rows.addAll(ret);
            }

            long count = rows.size();

            logger.info("Building dictionary for column: {}, its cardinality is: {}", col.getIdentity(), count);

            if (count > KapConfig.wrap(seg.getConfig()).getBuildDictionaryThreshold()) {
                logger.info(
                        "Warning: It's very risky to build the dictionary for column: {}, while its \n"
                                + "cardinality={} is very high, might cause out of memory exception.",
                        col.getIdentity(), count);
            }

            dictionaryMap.put(col, NDictionaryManager.buildDictionary(col, dictInfo, dictionaryBuilderClass,
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
                                        final String row = rows.get(i++);
                                        return row != null ? row : null;
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
        NDataSegment segCopy = writeDictionary(seg, dictionaryMap, start, end);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getName());
        update.setToUpdateSegs(segCopy);
        NDataflow updatedDataflow = NDataflowManager.getInstance(seg.getConfig(), dataflow.getProject())
                .updateDataflow(update);
        return updatedDataflow.getSegment(seg.getId());
    }

    private NDataSegment writeDictionary(NDataSegment segment, Map<TblColRef, Dictionary<String>> dictionaryMap,
            long startOffset, long endOffset) {

        // make a copy of the changing segment, avoid changing the cached object
        NDataflow dfCopy = segment.getDataflow().copy();
        NDataSegment segCopy = dfCopy.getSegment(segment.getId());

        for (Map.Entry<TblColRef, Dictionary<String>> entry : dictionaryMap.entrySet()) {
            final TblColRef tblColRef = entry.getKey();
            final Dictionary<String> dictionary = entry.getValue();
            IReadableTable.TableSignature signature = new IReadableTable.TableSignature();
            signature.setLastModifiedTime(System.currentTimeMillis());
            signature.setPath(String.format("streaming_%s_%s", startOffset, endOffset));
            signature.setSize(endOffset - startOffset);
            NDictionaryInfo dictInfo = new NDictionaryInfo(tblColRef.getColumnDesc(), tblColRef.getDatatype(),
                    signature, seg.getProject());
            logger.info("writing dictionary for TblColRef:" + tblColRef.toString());
            NDictionaryManager dictionaryManager = NDictionaryManager.getInstance(segment.getConfig(),
                    segment.getProject());
            try {
                NDictionaryInfo realDict = dictionaryManager.trySaveNewDict(dictionary, dictInfo);
                segCopy.putDictResPath(tblColRef, realDict.getResourcePath());
            } catch (IOException e) {
                throw new RuntimeException("error save dictionary for column:" + tblColRef, e);
            }
        }
        return segCopy;
    }

    public static boolean isUsingGlobalDict2(String dictBuildClz) {
        if (dictBuildClz == null) {
            return false;
        }

        INDictionaryBuilder builder = (INDictionaryBuilder) ClassUtil.newInstance(dictBuildClz);
        if (builder instanceof NGlobalDictionaryBuilder2)
            return true;

        return false;
    }

    public static TblColRef needGlobalDictionary(MeasureDesc measure) {
        String returnDataTypeName = measure.getFunction().getReturnDataType().getName();
        if (returnDataTypeName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
            List<TblColRef> cols = measure.getFunction().getParameter().getColRefs();
            Preconditions.checkArgument(cols.size() == 1);
            return cols.get(0);
        }
        return null;
    }
}
