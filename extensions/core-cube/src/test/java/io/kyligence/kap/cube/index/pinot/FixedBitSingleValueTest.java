/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.cube.index.pinot;

import java.io.File;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class FixedBitSingleValueTest {

    @Test
    public void testV2() throws Exception {

        int[] ROWS_LIST = { 1, 100, 1000, 10000, 100000 };
        for (int ROWS : ROWS_LIST) {
            for (int numBits = 0; numBits < 32; numBits++) {
                File file = new File("/tmp/" + this.getClass().getName() + "_" + numBits + ".test");
                FixedBitSingleValueWriter writer = new FixedBitSingleValueWriter(file, numBits);
                int data[] = new int[ROWS];
                Random random = new Random();
                int max = (int) Math.pow(2, numBits);
                for (int i = 0; i < ROWS; i++) {
                    data[i] = random.nextInt(max);
                    writer.setInt(i, data[i]);
                }
                writer.close();

                FixedBitSingleValueReader reader = FixedBitSingleValueReader.forHeap(file, numBits);
                Assert.assertEquals(ROWS, reader.getNumberOfRows());
                int[] read = new int[ROWS];
                for (int i = 0; i < ROWS; i++) {
                    read[i] = reader.getInt(i);
                    Assert.assertEquals(data[i], reader.getInt(i));
                }
                reader.close();

                file.delete();
            }
        }
    }
}
