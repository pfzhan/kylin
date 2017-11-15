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
//
//package org.apache.spark.sql.execution.datasources.sparder;
//
//import java.io.IOException;
//
//import org.apache.spark.SparkException;
//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
//import org.apache.spark.sql.types.Decimal;
//import org.apache.spark.unsafe.Platform;
//
//final class SparderGeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
//    private Object[] references;
//    private scala.collection.Iterator[] inputs;
//    private boolean agg_initAgg;
//    private boolean agg_bufIsNull;
//    private long agg_bufValue;
//    private boolean agg_bufIsNull1;
//    private Decimal agg_bufValue1;
//    private org.apache.spark.sql.execution.aggregate.HashAggregateExec agg_plan;
//    private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap;
//    private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter;
//    private org.apache.spark.unsafe.KVIterator agg_mapIter;
//    private org.apache.spark.sql.execution.metric.SQLMetric agg_peakMemory;
//    private org.apache.spark.sql.execution.metric.SQLMetric agg_spillSize;
//    private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
//    private scala.collection.Iterator scan_input;
//    private UnsafeRow agg_result;
//    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder;
//    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;
//    private int agg_value8;
//    private org.apache.spark.sql.catalyst.expressions.ScalaUDF agg_scalaUDF;
//    private scala.Function1 agg_catalystConverter;
//    private scala.Function1 agg_converter;
//    private scala.Function1 agg_converter1;
//    private scala.Function2 agg_udf;
//    private org.apache.spark.sql.catalyst.expressions.ScalaUDF agg_scalaUDF1;
//    private scala.Function1 agg_catalystConverter1;
//    private scala.Function1 agg_converter2;
//    private scala.Function1 agg_converter3;
//    private scala.Function2 agg_udf1;
//    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner agg_unsafeRowJoiner;
//    private org.apache.spark.sql.execution.metric.SQLMetric wholestagecodegen_numOutputRows;
//    private org.apache.spark.sql.execution.metric.SQLMetric wholestagecodegen_aggTime;
//
//    public SparderGeneratedIterator(Object[] references) {
//        this.references = references;
//    }
//
//    public void init(int index, scala.collection.Iterator[] inputs) {
//        partitionIndex = index;
//        this.inputs = inputs;
//        wholestagecodegen_init_0();
//        wholestagecodegen_init_1();
//        wholestagecodegen_init_2();
//
//    }
//
//    private void wholestagecodegen_init_0() {
//        agg_initAgg = false;
//
//        this.agg_plan = (org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0];
//
//        this.agg_peakMemory = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
//        this.agg_spillSize = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
//        this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[3];
//        scan_input = inputs[0];
//        agg_result = new UnsafeRow(2);
//        this.agg_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result, 64);
//        this.agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder, 2);
//
//        this.agg_scalaUDF = (org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[4];
//        this.agg_catalystConverter = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToCatalystConverter(agg_scalaUDF.dataType());
//        this.agg_converter = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToScalaConverter(
//                        ((org.apache.spark.sql.catalyst.expressions.Expression) (((org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[4])
//                                .getChildren().apply(0))).dataType());
//
//    }
//
//    private void agg_doAggregs() throws IOException, SparkException {
//        agg_hashMap = agg_plan.createHashMap();
//
//        while (scan_input.hasNext()) {
//            InternalRow scan_row = (InternalRow) scan_input.next();
//            scan_numOutputRows.add(1);
//            boolean scan_isNull4 = scan_row.isNullAt(0);
//            byte[] scan_value4 = scan_isNull4 ? null : (scan_row.getBinary(0));
//            boolean scan_isNull5 = scan_row.isNullAt(1);
//            byte[] scan_value5 = scan_isNull5 ? null : (scan_row.getBinary(1));
//            boolean scan_isNull6 = scan_row.isNullAt(2);
//            byte[] scan_value6 = scan_isNull6 ? null : (scan_row.getBinary(2));
//            boolean scan_isNull7 = scan_row.isNullAt(3);
//            byte[] scan_value7 = scan_isNull7 ? null : (scan_row.getBinary(3));
//
//            UnsafeRow agg_unsafeRowAggBuffer = null;
//
//            UnsafeRow agg_fastAggBuffer = null;
//
//            if (agg_fastAggBuffer == null) {
//                // generate grouping key
//                agg_holder.reset();
//
//                agg_rowWriter.zeroOutNullBytes();
//
//                if (scan_isNull4) {
//                    agg_rowWriter.setNullAt(0);
//                } else {
//                    agg_rowWriter.write(0, scan_value4);
//                }
//
//                if (scan_isNull5) {
//                    agg_rowWriter.setNullAt(1);
//                } else {
//                    agg_rowWriter.write(1, scan_value5);
//                }
//                agg_result.setTotalSize(agg_holder.totalSize());
//                agg_value8 = 42;
//
//                if (!scan_isNull4) {
//                    agg_value8 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(scan_value4,
//                            Platform.BYTE_ARRAY_OFFSET, scan_value4.length, agg_value8);
//                }
//
//                if (!scan_isNull5) {
//                    agg_value8 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(scan_value5,
//                            Platform.BYTE_ARRAY_OFFSET, scan_value5.length, agg_value8);
//                }
//                if (true) {
//                    // try to get the buffer from hash map
//                    agg_unsafeRowAggBuffer = agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result, agg_value8);
//                }
//                if (agg_unsafeRowAggBuffer == null) {
//                    if (agg_sorter == null) {
//                        agg_sorter = agg_hashMap.destructAndCreateExternalSorter();
//                    } else {
//                        agg_sorter.merge(agg_hashMap.destructAndCreateExternalSorter());
//                    }
//
//                    // the hash map had be spilled, it should have enough memory now,
//                    // try  to allocate buffer again.
//                    agg_unsafeRowAggBuffer = agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result, agg_value8);
//                    if (agg_unsafeRowAggBuffer == null) {
//                        // failed to allocate the first page
//                        throw new OutOfMemoryError("No enough memory for aggregation");
//                    }
//                }
//            }
//
//            if (agg_fastAggBuffer != null) {
//                // update fast row
//
//            } else {
//                // update unsafe row
//
//                // common sub-expressions
//
//                // evaluate aggregate function
//                boolean agg_isNull10 = true;
//                long agg_value12 = -1L;
//
//                boolean agg_isNull12 = agg_unsafeRowAggBuffer.isNullAt(0);
//                long agg_value14 = agg_isNull12 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
//                boolean agg_isNull11 = agg_isNull12;
//                long agg_value13 = agg_value14;
//                if (agg_isNull11) {
//                    boolean agg_isNull13 = false;
//                    long agg_value15 = -1L;
//                    if (!false) {
//                        agg_value15 = (long) 0;
//                    }
//                    if (!agg_isNull13) {
//                        agg_isNull11 = false;
//                        agg_value13 = agg_value15;
//                    }
//                }
//
//                Object agg_arg = scan_isNull6 ? null : agg_converter.apply(scan_value6);
//                Object agg_arg1 = false ? null : agg_converter1.apply(4);
//
//                Long agg_result1 = null;
//                try {
//                    agg_result1 = (Long) agg_catalystConverter.apply(agg_udf.apply(agg_arg, agg_arg1));
//                } catch (Exception e) {
//                    throw new org.apache.spark.SparkException(agg_scalaUDF.udfErrorMessage(), e);
//                }
//
//                boolean agg_isNull16 = agg_result1 == null;
//                long agg_value18 = -1L;
//                if (!agg_isNull16) {
//                    agg_value18 = agg_result1;
//                }
//                boolean agg_isNull15 = agg_isNull16;
//                long agg_value17 = -1L;
//                if (!agg_isNull16) {
//                    agg_value17 = agg_value18;
//                }
//                if (!agg_isNull15) {
//                    agg_isNull10 = false; // resultCode could change nullability.
//                    agg_value12 = agg_value13 + agg_value17;
//
//                }
//                boolean agg_isNull9 = agg_isNull10;
//                long agg_value11 = agg_value12;
//                if (agg_isNull9) {
//                    boolean agg_isNull19 = agg_unsafeRowAggBuffer.isNullAt(0);
//                    long agg_value21 = agg_isNull19 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
//                    if (!agg_isNull19) {
//                        agg_isNull9 = false;
//                        agg_value11 = agg_value21;
//                    }
//                }
//                boolean agg_isNull21 = true;
//                Decimal agg_value23 = null;
//
//                boolean agg_isNull23 = agg_unsafeRowAggBuffer.isNullAt(1);
//                Decimal agg_value25 = agg_isNull23 ? null : (agg_unsafeRowAggBuffer.getDecimal(1, 29, 4));
//                boolean agg_isNull22 = agg_isNull23;
//                Decimal agg_value24 = agg_value25;
//                if (agg_isNull22) {
//                    boolean agg_isNull24 = false;
//                    Decimal agg_value26 = null;
//                    if (!false) {
//                        Decimal agg_tmpDecimal = Decimal.apply((long) 0);
//
//                        if (agg_tmpDecimal.changePrecision(29, 4)) {
//                            agg_value26 = agg_tmpDecimal;
//                        } else {
//                            agg_isNull24 = true;
//                        }
//
//                    }
//                    if (!agg_isNull24) {
//                        agg_isNull22 = false;
//                        agg_value24 = agg_value26;
//                    }
//                }
//                if (!agg_isNull22) {
//                    Object agg_arg2 = scan_isNull7 ? null : agg_converter2.apply(scan_value7);
//                    Object agg_arg3 = false ? null : agg_converter3.apply(6);
//
//                    Decimal agg_result2 = null;
//                    try {
//                        agg_result2 = (Decimal) agg_catalystConverter1.apply(agg_udf1.apply(agg_arg2, agg_arg3));
//                    } catch (Exception e) {
//                        throw new org.apache.spark.SparkException(agg_scalaUDF1.udfErrorMessage(), e);
//                    }
//
//                    boolean agg_isNull27 = agg_result2 == null;
//                    Decimal agg_value29 = null;
//                    if (!agg_isNull27) {
//                        agg_value29 = agg_result2;
//                    }
//                    boolean agg_isNull26 = agg_isNull27;
//                    Decimal agg_value28 = null;
//                    if (!agg_isNull27) {
//                        Decimal agg_tmpDecimal1 = agg_value29.clone();
//
//                        if (agg_tmpDecimal1.changePrecision(29, 4)) {
//                            agg_value28 = agg_tmpDecimal1;
//                        } else {
//                            agg_isNull26 = true;
//                        }
//
//                    }
//                    if (!agg_isNull26) {
//                        agg_isNull21 = false; // resultCode could change nullability.
//                        agg_value23 = agg_value24.$plus(agg_value28);
//
//                    }
//
//                }
//                boolean agg_isNull20 = agg_isNull21;
//                Decimal agg_value22 = agg_value23;
//                if (agg_isNull20) {
//                    boolean agg_isNull30 = agg_unsafeRowAggBuffer.isNullAt(1);
//                    Decimal agg_value32 = agg_isNull30 ? null : (agg_unsafeRowAggBuffer.getDecimal(1, 29, 4));
//                    if (!agg_isNull30) {
//                        agg_isNull20 = false;
//                        agg_value22 = agg_value32;
//                    }
//                }
//                // update unsafe row buffer
//                if (!agg_isNull9) {
//                    agg_unsafeRowAggBuffer.setLong(0, agg_value11);
//                } else {
//                    agg_unsafeRowAggBuffer.setNullAt(0);
//                }
//
//                if (!agg_isNull20) {
//                    agg_unsafeRowAggBuffer.setDecimal(1, agg_value22, 29);
//                } else {
//                    agg_unsafeRowAggBuffer.setDecimal(1, null, 29);
//                }
//
//            }
//            if (shouldStop())
//                return;
//        }
//
//        agg_mapIter = agg_plan.finishAggregate(agg_hashMap, agg_sorter, agg_peakMemory, agg_spillSize);
//    }
//
//    private void wholestagecodegen_init_2() {
//        this.agg_udf1 = (scala.Function2) agg_scalaUDF1.userDefinedFunc();
//        agg_unsafeRowJoiner = agg_plan.createUnsafeJoiner();
//        this.wholestagecodegen_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[6];
//        this.wholestagecodegen_aggTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[7];
//
//    }
//
//    private void wholestagecodegen_init_1() {
//        this.agg_converter1 = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToScalaConverter(
//                        ((org.apache.spark.sql.catalyst.expressions.Expression) (((org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[4])
//                                .getChildren().apply(1))).dataType());
//        this.agg_udf = (scala.Function2) agg_scalaUDF.userDefinedFunc();
//        this.agg_scalaUDF1 = (org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[5];
//        this.agg_catalystConverter1 = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToCatalystConverter(agg_scalaUDF1.dataType());
//        this.agg_converter2 = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToScalaConverter(
//                        ((org.apache.spark.sql.catalyst.expressions.Expression) (((org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[5])
//                                .getChildren().apply(0))).dataType());
//        this.agg_converter3 = (scala.Function1) org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$
//                .createToScalaConverter(
//                        ((org.apache.spark.sql.catalyst.expressions.Expression) (((org.apache.spark.sql.catalyst.expressions.ScalaUDF) references[5])
//                                .getChildren().apply(1))).dataType());
//
//    }
//
//    protected void processNext() throws java.io.IOException {
//        if (!agg_initAgg) {
//            agg_initAgg = true;
//            long wholestagecodegen_beforeAgg = System.nanoTime();
//            try {
//                agg_doAggregs();
//            } catch (SparkException e) {
//                e.printStackTrace();
//            }
//            wholestagecodegen_aggTime.add((System.nanoTime() - wholestagecodegen_beforeAgg) / 1000000);
//        }
//
//        // output the result
//
//        while (agg_mapIter.next()) {
//            wholestagecodegen_numOutputRows.add(1);
//            UnsafeRow agg_aggKey = (UnsafeRow) agg_mapIter.getKey();
//            UnsafeRow agg_aggBuffer = (UnsafeRow) agg_mapIter.getValue();
//
//            UnsafeRow agg_resultRow = agg_unsafeRowJoiner.join(agg_aggKey, agg_aggBuffer);
//
//            append(agg_resultRow);
//
//            if (shouldStop())
//                return;
//        }
//
//        agg_mapIter.close();
//        if (agg_sorter == null) {
//            agg_hashMap.free();
//        }
//    }
//}
