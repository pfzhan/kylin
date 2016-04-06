package io.kyligence.kap.cube.index;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueReader;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueWriter;

/**
 * Created by dongli on 4/6/16.
 */
public class GTColumnForwordIndexTest {

//    @Test
//    public void testV2() throws Exception {
//        int ROWS = 1000;
//        for (int numBits = 1; numBits < 32; numBits++) {
//            File file = new File("/tmp/" + this.getClass().getName() + "_" + numBits + ".test");
//            FixedBitSingleValueWriter writer = new FixedBitSingleValueWriter(file, ROWS, numBits);
//            int data[] = new int[ROWS];
//            Random random = new Random();
//            int max = (int) Math.pow(2, numBits);
//            for (int i = 0; i < ROWS; i++) {
//                data[i] = random.nextInt(max);
//                writer.setInt(i, data[i]);
//            }
//            writer.close();
//            FixedBitSingleValueReader reader = FixedBitSingleValueReader.forHeap(file, ROWS, numBits);
//            int[] read = new int[ROWS];
//            for (int i = 0; i < ROWS; i++) {
//                read[i] = reader.getInt(i);
//                Assert.assertEquals(data[i], reader.getInt(i));
//            }
//            System.out.println(Arrays.toString(data));
//            System.out.println(Arrays.toString(read));
//            reader.close();
//            file.delete();
//        }
//    }
}
