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
package org.apache.spark.sql.execution.datasources.sparder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class ArrayBenchmark {
    private static final int RUNS = 1;
    private static final int DIMENSION_1 = 62;
    private static final int DIMENSION_2 = 1024;
    private static final int col = 10;
    private static final int rowCount = 1024 * 1024;
    private static final int BUFFER_SIZE = 1024;

    private static long[][] longs;
    private static byte[][][] row;

    public static void main(String[] args) throws Exception {

        testByteAllocate();
    }



    public static void testByteAllocate(){
        long start = System.nanoTime();
        for(int i =0 ;i< 100; i++) {
            int[] ints = new int[10000];
        }
        System.out.println((System.nanoTime() - start)/100);
        start = System.nanoTime();
        for(int i =0 ;i< 100; i++) {
            byte[] bytes = new byte[40000];
        }
        System.out.println((System.nanoTime() - start)/100);


    }
    public static void testInteger() throws InterruptedException {
        DataType anInt = DataType.getType("integer");
        DataTypeSerializer<Object>[] dataTypeSerializers = new DataTypeSerializer[col];
        for (int i = 0; i < col; i++) {
            dataTypeSerializers[i] = (DataTypeSerializer<Object>) new IntegerDimEnc(8).asDataTypeSerializer();
        }

        Random random = new Random(100000);
        ByteBuffer byteBuffer = ByteBuffer.allocate(64);
        row = new byte[col][][];
        for (int i = 0; i < col; i++) {
            row[i] = new byte[rowCount][];
            for (int j = 0; j < rowCount; j++) {
                byteBuffer.clear();
                dataTypeSerializers[i].serialize(random.nextInt(), byteBuffer);
                row[i][j] = Arrays.copyOf(byteBuffer.array(), byteBuffer.position());
            }
        }
        long count = 0;
        long totalTime = 0;
        Thread.sleep(10000);
        System.out.println("starting....");
        long start = System.nanoTime();
        //        for (int r = 0; r < RUNS; r++) {
        //            for (int i = 0; i < col; i++) {
        //                for (int j = 0; j < rowCount; j++) {
        //                    long time = System.nanoTime();
        //                    Double deserialize = dataTypeSerializers[i].deserialize(ByteBuffer.wrap(row[i][j]));
        //                    count++;
        //                    totalTime += System.nanoTime() - time;
        //                    if (count % 10000000 == 0) {
        //                        System.out.println(totalTime / count);
        //                        totalTime = 0;
        //                        count = 0;
        //                    }
        //                }
        //            }
        //        }
        //        System.out.println("duration = " + (System.nanoTime() - start) / 1000000);

        //                start = System.nanoTime();
        for (int r = 0; r < RUNS; r++) {
            for (int i = 0; i < rowCount; i++) {
                for (int j = 0; j < col; j++) {
                    long time = System.nanoTime();
                    Object deserialize = ByteBuffer.wrap(row[j][i]).getLong();
                    count++;
                    totalTime += System.nanoTime() - time;
                    if (count % 10000000 == 0) {
                        System.out.println(totalTime / count);
                        totalTime = 0;
                        count = 0;
                    }
                }
            }
        }
        //
        //        long sum = 0L;
        //        for (int r = 0; r < RUNS; r++) {
        //            for (int j = 0; j < DIMENSION_2; j++) {
        //                for (int i = 0; i < DIMENSION_1; i++) {
        //                    sum += longs[i][j];
        //                }
        //            }

        //            for (int i = 0; i < DIMENSION_1; i++) {
        //                for (int j = 0; j < DIMENSION_2; j++) {
        //                    sum += longs[i][j];
        //                }
        //            }
        //        }
        System.out.println("duration = " + (System.nanoTime() - start) / 1000000);
    }

}