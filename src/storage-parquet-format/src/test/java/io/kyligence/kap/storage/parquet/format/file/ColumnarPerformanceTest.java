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

package io.kyligence.kap.storage.parquet.format.file;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import sun.misc.Unsafe;

/**
 * 
1.filterRowLayout - running the 1st time ... 
Filtered rows 35027510
1.filterRowLayout - processed 70000000 rows in 3.123 seconds, 22.414345 million rows/s
1.filterRowLayout - running the 2nd time ... 
Filtered rows 35027510
1.filterRowLayout - processed 70000000 rows in 3.123 seconds, 22.414345 million rows/s
1.filterRowLayout - running the 3rd time ... 
Filtered rows 35027510
1.filterRowLayout - processed 70000000 rows in 2.402 seconds, 29.142382 million rows/s
2.filterCompactRowLayout - running the 1st time ... 
Filtered rows 35027510
2.filterCompactRowLayout - processed 70000000 rows in 0.331 seconds, 211.48036 million rows/s
2.filterCompactRowLayout - running the 2nd time ... 
Filtered rows 35027510
2.filterCompactRowLayout - processed 70000000 rows in 0.372 seconds, 188.17204 million rows/s
2.filterCompactRowLayout - running the 3rd time ... 
Filtered rows 35027510
2.filterCompactRowLayout - processed 70000000 rows in 0.314 seconds, 222.92993 million rows/s
3.filterCompactRowLayoutUsingBuffer - running the 1st time ... 
Filtered rows 35027510
3.filterCompactRowLayoutUsingBuffer - processed 70000000 rows in 0.294 seconds, 238.09525 million rows/s
3.filterCompactRowLayoutUsingBuffer - running the 2nd time ... 
Filtered rows 35027510
3.filterCompactRowLayoutUsingBuffer - processed 70000000 rows in 0.299 seconds, 234.11371 million rows/s
3.filterCompactRowLayoutUsingBuffer - running the 3rd time ... 
Filtered rows 35027510
3.filterCompactRowLayoutUsingBuffer - processed 70000000 rows in 0.263 seconds, 266.1597 million rows/s
3e.filterCompactRowLayoutUsingCustomGetInt - running the 1st time ... 
Filtered rows 35027510
3e.filterCompactRowLayoutUsingCustomGetInt - processed 70000000 rows in 0.2 seconds, 350.0 million rows/s
3e.filterCompactRowLayoutUsingCustomGetInt - running the 2nd time ... 
Filtered rows 35027510
3e.filterCompactRowLayoutUsingCustomGetInt - processed 70000000 rows in 0.2 seconds, 350.0 million rows/s
3e.filterCompactRowLayoutUsingCustomGetInt - running the 3rd time ... 
Filtered rows 35027510
3e.filterCompactRowLayoutUsingCustomGetInt - processed 70000000 rows in 0.201 seconds, 348.2587 million rows/s
4.filterColumnBytesUsingByteBuffer - running the 1st time ... 
Filtered rows 35027510
4.filterColumnBytesUsingByteBuffer - processed 70000000 rows in 0.237 seconds, 295.35864 million rows/s
4.filterColumnBytesUsingByteBuffer - running the 2nd time ... 
Filtered rows 35027510
4.filterColumnBytesUsingByteBuffer - processed 70000000 rows in 0.285 seconds, 245.61403 million rows/s
4.filterColumnBytesUsingByteBuffer - running the 3rd time ... 
Filtered rows 35027510
4.filterColumnBytesUsingByteBuffer - processed 70000000 rows in 0.265 seconds, 264.15094 million rows/s
5.filterColumnBytesUsingIntBuffer - running the 1st time ... 
Filtered rows 35027510
5.filterColumnBytesUsingIntBuffer - processed 70000000 rows in 0.149 seconds, 469.79865 million rows/s
5.filterColumnBytesUsingIntBuffer - running the 2nd time ... 
Filtered rows 35027510
5.filterColumnBytesUsingIntBuffer - processed 70000000 rows in 0.2 seconds, 350.0 million rows/s
5.filterColumnBytesUsingIntBuffer - running the 3rd time ... 
Filtered rows 35027510
5.filterColumnBytesUsingIntBuffer - processed 70000000 rows in 0.1 seconds, 700.0 million rows/s
6.filterColumnBytesUsingUnsafe (slow & broken) - running the 1st time ... 
Filtered rows 34921530
6.filterColumnBytesUsingUnsafe (slow & broken) - processed 70000000 rows in 0.284 seconds, 246.47887 million rows/s
6.filterColumnBytesUsingUnsafe (slow & broken) - running the 2nd time ... 
Filtered rows 34921530
6.filterColumnBytesUsingUnsafe (slow & broken) - processed 70000000 rows in 0.379 seconds, 184.69656 million rows/s
6.filterColumnBytesUsingUnsafe (slow & broken) - running the 3rd time ... 
Filtered rows 34921530
6.filterColumnBytesUsingUnsafe (slow & broken) - processed 70000000 rows in 0.338 seconds, 207.10059 million rows/s
7.filterColumnIntsUsingUnsafe - running the 1st time ... 
Filtered rows 35027510
7.filterColumnIntsUsingUnsafe - processed 70000000 rows in 0.047 seconds, 1489.3617 million rows/s
7.filterColumnIntsUsingUnsafe - running the 2nd time ... 
Filtered rows 35027510
7.filterColumnIntsUsingUnsafe - processed 70000000 rows in 0.053 seconds, 1320.7548 million rows/s
7.filterColumnIntsUsingUnsafe - running the 3rd time ... 
Filtered rows 35027510
7.filterColumnIntsUsingUnsafe - processed 70000000 rows in 0.031 seconds, 2258.0645 million rows/s
8.filterColumnIntsLikeCompiledCode - running the 1st time ... 
Filtered rows 35027510
8.filterColumnIntsLikeCompiledCode - processed 70000000 rows in 0.031 seconds, 2258.0645 million rows/s
8.filterColumnIntsLikeCompiledCode - running the 2nd time ... 
Filtered rows 35027510
8.filterColumnIntsLikeCompiledCode - processed 70000000 rows in 0.054 seconds, 1296.2963 million rows/s
8.filterColumnIntsLikeCompiledCode - running the 3rd time ... 
Filtered rows 35027510
8.filterColumnIntsLikeCompiledCode - processed 70000000 rows in 0.084 seconds, 833.3333 million rows/s
 *
 */
@Ignore("Performance test don't run by default")
public class ColumnarPerformanceTest {

    public interface TestToRepeat {
        long test();
    }

    public static void repeat(String testName, TestToRepeat test) {
        long t, cnt;

        long t0 = System.currentTimeMillis();
        System.out.println(testName + " - running the 1st time ... ");
        cnt = test.test();
        t = System.currentTimeMillis() - t0;
        System.out.println(testName + " - processed " + cnt + " rows in " + ((float) t / 1000) + " seconds, "
                + ((float) cnt / 1000 / t) + " million rows/s");

        long t1 = System.currentTimeMillis();
        System.out.println(testName + " - running the 2nd time ... ");
        cnt = test.test();
        t = System.currentTimeMillis() - t1;
        System.out.println(testName + " - processed " + cnt + " rows in " + ((float) t / 1000) + " seconds, "
                + ((float) cnt / 1000 / t) + " million rows/s");

        long t2 = System.currentTimeMillis();
        System.out.println(testName + " - running the 3rd time ... ");
        cnt = test.test();
        t = System.currentTimeMillis() - t2;
        System.out.println(testName + " - processed " + cnt + " rows in " + ((float) t / 1000) + " seconds, "
                + ((float) cnt / 1000 / t) + " million rows/s");
    }

    // ============================================================================

    int testRows = 1000000;
    int scanCycles = 70;

    int rowLen = 40;
    List<ByteArray> rowLayout;
    byte[] compactRowLayout;

    int colLen = 4;
    byte[] colLayout;
    int[] compiledColLayout;

    Unsafe unsafe = getUnsafe();

    private void setup() {

        rowLayout = randomBytes(testRows, rowLen);

        // build compact row layout
        compactRowLayout = new byte[testRows * rowLen];
        for (int i = 0; i < testRows; i++) {
            System.arraycopy(rowLayout.get(i).array(), 0, compactRowLayout, i * rowLen, rowLen);
        }

        // build column layout, with the first column
        colLayout = new byte[testRows * colLen];
        for (int i = 0; i < testRows; i++) {
            System.arraycopy(rowLayout.get(i).array(), 0, colLayout, i * colLen, colLen);
        }

        // build column layout in compiled type
        compiledColLayout = new int[testRows];
        for (int i = 0; i < testRows; i++) {
            compiledColLayout[i] = rowLayout.get(i).asBuffer().getInt();
        }
    }

    private List<ByteArray> randomBytes(int rowCnt, int rowLen) {
        List<ByteArray> arrList = new ArrayList<>(rowCnt);
        Random rand = new Random();
        for (int i = 0; i < rowCnt; i++) {
            ByteArray arr = new ByteArray(rowLen);
            rand.nextBytes(arr.array());
            arrList.add(arr);
        }
        Collections.sort(arrList);

        return arrList;
    }

    private Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Test
    public void test() {
        setup();

        repeat("1.filterRowLayout", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    for (int i = 0; i < testRows; i++) {
                        ByteArray row = rowLayout.get(i);
                        int v = Bytes.readAsInt(row.array(), 0, colLen);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("2.filterCompactRowLayout", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    for (int i = 0, offset = 0; i < testRows; i++, offset += rowLen) {
                        int v = Bytes.readAsInt(compactRowLayout, offset, colLen);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("3.filterCompactRowLayoutUsingBuffer", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    ByteBuffer buf = ByteBuffer.wrap(compactRowLayout);
                    for (int i = 0, offset = 0; i < testRows; i++, offset += rowLen) {
                        int v = buf.getInt(offset);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("3e.filterCompactRowLayoutUsingCustomGetInt", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    for (int i = 0, offset = 0; i < testRows; i++, offset += rowLen) {
                        int v = getIntB(compactRowLayout, offset);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("4.filterColumnBytesUsingByteBuffer", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    ByteBuffer buf = ByteBuffer.wrap(colLayout);
                    for (int i = 0, offset = 0; i < testRows; i++, offset += colLen) {
                        int v = buf.getInt(offset);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("5.filterColumnBytesUsingIntBuffer", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    IntBuffer buf = ByteBuffer.wrap(colLayout).asIntBuffer();
                    for (int i = 0; i < testRows; i++) {
                        int v = buf.get(i);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("6.filterColumnBytesUsingUnsafe (slow & broken)", new TestToRepeat() {

            @Override
            public long test() {
                int scale = unsafe.arrayIndexScale(colLayout.getClass());
                int len = scale * colLen;

                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    long offset = unsafe.arrayBaseOffset(colLayout.getClass());
                    for (int i = 0; i < testRows; i++, offset += len) {
                        int v = unsafe.getInt(colLayout, offset);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("7.filterColumnIntsUsingUnsafe", new TestToRepeat() {

            @Override
            public long test() {
                int scale = unsafe.arrayIndexScale(compiledColLayout.getClass());

                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    long offset = unsafe.arrayBaseOffset(compiledColLayout.getClass());
                    for (int i = 0; i < testRows; i++, offset += scale) {
                        int v = unsafe.getInt(compiledColLayout, offset);
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });

        repeat("8.filterColumnIntsLikeCompiledCode", new TestToRepeat() {

            @Override
            public long test() {
                long filteredCnt = 0;
                for (int cycle = 0; cycle < scanCycles; cycle++) {
                    for (int i = 0; i < testRows; i++) {
                        int v = compiledColLayout[i];
                        if (filter(v))
                            filteredCnt++;
                    }
                }
                System.out.println("Filtered rows " + filteredCnt);
                return (long) scanCycles * testRows;
            }

        });
    }

    static int getIntB(byte[] bb, int bi) {
        return makeInt(bb[bi], bb[bi + 1], bb[bi + 2], bb[bi + 3]);
    }

    static private int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3) << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | ((b0 & 0xff)));
    }

    private boolean filter(int v) {
        return v > 1000;
    }
}
