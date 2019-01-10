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

import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;

public class NLayoutMergeAssist implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(NLayoutMergeAssist.class);
    private static final int DEFAULT_BUFFER_SIZE = 256;
    private LayoutEntity layout;
    private NDataSegment newSegment;
    private List<NDataSegment> toMergeSegments;
    private SparkSession ss;
    final private List<NDataLayout> toMergeCuboids = new ArrayList<>();

    public void setSs(SparkSession ss) {
        this.ss = ss;
    }

    public void setLayout(LayoutEntity layout) {
        this.layout = layout;
    }

    public LayoutEntity getLayout() {
        return this.layout;
    }

    public List<NDataLayout> getCuboids() {
        return this.toMergeCuboids;
    }

    public void addCuboid(NDataLayout cuboid) {
        toMergeCuboids.add(cuboid);
    }

    public void setToMergeSegments(List<NDataSegment> segments) {
        this.toMergeSegments = segments;
    }

    public void setNewSegment(NDataSegment segment) {
        this.newSegment = segment;
    }

    public Dataset<Row> merge() {
        Dataset<Row> mergeDataset = null;
        for (int i = 0; i < toMergeCuboids.size(); i++) {
            Dataset<Row> layoutDataset = StorageFactory
                    .createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                    .getFrom(NSparkCubingUtil.getStoragePath(toMergeCuboids.get(i)), ss);

            layoutDataset = rewriteDict(layoutDataset, toMergeSegments.get(i));
            if (mergeDataset == null) {
                mergeDataset = layoutDataset;
            } else
                mergeDataset = mergeDataset.union(layoutDataset);
        }
        return mergeDataset;

    }

    private Dataset<Row> rewriteDict(Dataset<Row> old, NDataSegment segment) {
        final Map<Integer, Dictionary<String>> oldDimsDicts = Maps.newConcurrentMap();
        final Map<Integer, Dictionary<String>> newDimsDicts = Maps.newConcurrentMap();
        final Map<TblColRef, Dictionary<String>> oldMeasureDicts = Maps.newConcurrentMap();
        final Map<TblColRef, Dictionary<String>> newMeasureDicts = Maps.newConcurrentMap();
        final Map<Integer, MeasureIngester> measureIngesters = Maps.newConcurrentMap();
        final int dimsSize = layout.getOrderedDimensions().size();
        final int measSize = layout.getOrderedMeasures().size();

        int position = 0;
        for (TblColRef col : layout.getOrderedDimensions().values()) {
            if (false == segment.getIndexPlan().getAllColumnsHaveDictionary().contains(col)) {
                position++;
                continue;
            }
            oldDimsDicts.put(position, segment.getDictionary(col));
            newDimsDicts.put(position, newSegment.getDictionary(col));
            position++;
        }

        final List<MeasureDesc> measureDescs = Lists.newArrayList();
        measureDescs.addAll(layout.getOrderedMeasures().values());
        final MeasureCodec measureCodec = new MeasureCodec(measureDescs);
        for (int i = 0; i < measureDescs.size(); i++) {
            MeasureDesc measureDesc = measureDescs.get(i);
            MeasureType measureType = measureDesc.getFunction().getMeasureType();
            List<TblColRef> columns = measureType.getColumnsNeedDictionary(measureDesc.getFunction());
            boolean needRewrite = false;
            for (TblColRef col : columns) {
                if (false == segment.getIndexPlan().getAllColumnsHaveDictionary().contains(col)) {
                    continue;
                }

                oldMeasureDicts.put(col, segment.getDictionary(col));
                newMeasureDicts.put(col, newSegment.getDictionary(col));
                needRewrite = true;
            }

            if (needRewrite)
                measureIngesters.put(i, measureType.newIngester());
        }

        return old.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                int size = value.size();
                Object[] objs = new Object[size];
                for (int i = 0; i < dimsSize; i++) {
                    if (oldDimsDicts.keySet().contains(i)) {
                        byte[] bytes = (byte[]) value.get(i);
                        int idInSourceDict = BytesUtil.readUnsigned(bytes, 0, bytes.length);
                        int idInMergedDict;

                        String v = oldDimsDicts.get(i).getValueFromId(idInSourceDict);
                        if (v == null) {
                            idInMergedDict = newDimsDicts.get(i).nullId();
                        } else {
                            idInMergedDict = newDimsDicts.get(i).getIdFromValue(v);
                        }
                        int sizeOfId = newDimsDicts.get(i).getSizeOfId();
                        bytes = new byte[sizeOfId];
                        BytesUtil.writeUnsigned(idInMergedDict, bytes, 0, sizeOfId);
                        objs[i] = bytes;
                    } else
                        objs[i] = value.get(i);

                }

                for (int j = dimsSize; j < dimsSize + measSize; j++) {
                    int measurePos = j - dimsSize;
                    if (measureIngesters.keySet().contains(measurePos)) {
                        Object measureObj = measureCodec.decode(measurePos, ByteBuffer.wrap((byte[]) value.get(j)));
                        measureObj = measureIngesters.get(measurePos).reEncodeDictionary(measureObj,
                                measureDescs.get(measurePos), oldMeasureDicts, newMeasureDicts);
                        int estimateSize = DEFAULT_BUFFER_SIZE;
                        byte[] ret = null;
                        while (true) {
                            try {
                                ByteBuffer buf = ByteBuffer.allocate(estimateSize);
                                buf.clear();
                                measureCodec.encode(measurePos, measureObj, buf);
                                ret = new byte[buf.position()];
                                System.arraycopy(buf.array(), 0, ret, 0, buf.position());
                                break;
                            } catch (BufferOverflowException e) {
                                logger.info("Buffer size {} cannot hold the filter, resizing to 2 times", estimateSize);
                                if (estimateSize == (1 << 30))
                                    throw e;
                                estimateSize = estimateSize << 1;
                            }
                        }
                        objs[j] = ret;

                    } else {
                        objs[j] = value.get(j);
                    }

                }
                return RowFactory.create(objs);
            }
        }, RowEncoder.apply(old.schema()));
    }
}
