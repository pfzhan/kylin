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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
1.filterRowLayout - running the 1st time ... 
Filtered rows 24979400
1.filterRowLayout - processed 50000000 rows in 3.161 seconds, 15.81778 million rows/s
1.filterRowLayout - running the 2nd time ... 
Filtered rows 24979400
1.filterRowLayout - processed 50000000 rows in 3.169 seconds, 15.777848 million rows/s
1.filterRowLayout - running the 3rd time ... 
Filtered rows 24979400
1.filterRowLayout - processed 50000000 rows in 2.531 seconds, 19.755037 million rows/s
2.filterCompactRowLayout - running the 1st time ... 
Filtered rows 24979400
2.filterCompactRowLayout - processed 50000000 rows in 0.425 seconds, 117.64706 million rows/s
2.filterCompactRowLayout - running the 2nd time ... 
Filtered rows 24979400
2.filterCompactRowLayout - processed 50000000 rows in 0.422 seconds, 118.48341 million rows/s
2.filterCompactRowLayout - running the 3rd time ... 
Filtered rows 24979400
2.filterCompactRowLayout - processed 50000000 rows in 0.39 seconds, 128.20512 million rows/s
3.filterCompactRowLayoutUsingIntBuffer - running the 1st time ... 
Filtered rows 24979400
3.filterCompactRowLayoutUsingIntBuffer - processed 50000000 rows in 0.436 seconds, 114.6789 million rows/s
3.filterCompactRowLayoutUsingIntBuffer - running the 2nd time ... 
Filtered rows 24979400
3.filterCompactRowLayoutUsingIntBuffer - processed 50000000 rows in 0.386 seconds, 129.53368 million rows/s
3.filterCompactRowLayoutUsingIntBuffer - running the 3rd time ... 
Filtered rows 24979400
3.filterCompactRowLayoutUsingIntBuffer - processed 50000000 rows in 0.374 seconds, 133.68983 million rows/s
4.filterColumnLayout - running the 1st time ... 
Filtered rows 24979400
4.filterColumnLayout - processed 50000000 rows in 0.184 seconds, 271.73914 million rows/s
4.filterColumnLayout - running the 2nd time ... 
Filtered rows 24979400
4.filterColumnLayout - processed 50000000 rows in 0.187 seconds, 267.37967 million rows/s
4.filterColumnLayout - running the 3rd time ... 
Filtered rows 24979400
4.filterColumnLayout - processed 50000000 rows in 0.159 seconds, 314.46542 million rows/s
5.filterColumnLayoutLikeCompiledCode - running the 1st time ... 
Filtered rows 24979400
5.filterColumnLayoutLikeCompiledCode - processed 50000000 rows in 0.026 seconds, 1923.0769 million rows/s
5.filterColumnLayoutLikeCompiledCode - running the 2nd time ... 
Filtered rows 24979400
5.filterColumnLayoutLikeCompiledCode - processed 50000000 rows in 0.035 seconds, 1428.5714 million rows/s
5.filterColumnLayoutLikeCompiledCode - running the 3rd time ... 
Filtered rows 24979400
5.filterColumnLayoutLikeCompiledCode - processed 50000000 rows in 0.054 seconds, 925.9259 million rows/s
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
    int scanCycles = 50;

    int rowLen = 100;
    List<ByteArray> rowLayout;
    byte[] compactRowLayout;

    int colLen = 4;
    byte[] colLayout;
    int[] compiledColLayout;

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
        
        repeat("3.filterCompactRowLayoutUsingIntBuffer", new TestToRepeat() {
            
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
        
        repeat("4.filterColumnLayout", new TestToRepeat() {
            
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
        
        repeat("5.filterColumnLayoutLikeCompiledCode", new TestToRepeat() {
            
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
    
    private boolean filter(int v) {
        return v > 1000;
    }
}
