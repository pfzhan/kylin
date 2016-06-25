package io.kyligence.kap.cube.index;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import io.kyligence.kap.cube.index.IColumnForwardIndex.Builder;
import io.kyligence.kap.cube.index.IColumnForwardIndex.Reader;

/**
 * Similar to GTScannerBenchmark
 * 
 * - Benchmark of processing 10 million GTRecords. 5 dimensions of type int4, and 2 measures of type long8.
 * - Here processing is read from memory mapped file
 * - Enlarge measure to 16 bytes each as a penalty to the enforced fixed-length property
 * - Leverage Pinot storage, 4 bytes for each column, so use 5 + 4 + 4 = 13 columns in total
 */
public class MemMappedFileScanBenchmark {

    final int N = 10000000;
    final GTColumnForwardIndex[] columns = new GTColumnForwardIndex[5 + 4 + 4];

    public MemMappedFileScanBenchmark() throws IOException {
        for (int i = 0; i < columns.length; i++) {
            File tmpFile = File.createTempFile("MemMappedFileScanBenchmark", ".idx");
            columns[i] = new GTColumnForwardIndex("" + 0, -1, tmpFile.getAbsolutePath());
            fillRandom(columns[i]);
        }
    }

    private void fillRandom(GTColumnForwardIndex col) throws IOException {
        Random rand = new Random();
        Builder builder = col.rebuild();
        for (int i = 0; i < N; i++) {
            builder.putNextRow(Math.abs(rand.nextInt()));
        }
        builder.close();
    }

    @After
    public void cleanup() {
        for (int i = 0; i < columns.length; i++)
            columns[i].deleteIndexFile();
    }

    @Test
    public void test() {
        Reader[] readers = new Reader[columns.length];

        {
            long t = System.currentTimeMillis();
            for (int i = 0; i < readers.length; i++) {
                columns[i].setUseMmap(false);
                readers[i] = columns[i].getReader();
            }
            for (int i = 0; i < N; i++) {
                for (int c = 0; c < readers.length; c++)
                    readers[c].get(i);
            }
            t = System.currentTimeMillis() - t;
            System.out.println(N + " records read without mmap, " + calcSpeed(t) + "K rec/sec");
        }

        {
            long t = System.currentTimeMillis();
            for (int i = 0; i < readers.length; i++) {
                columns[i].setUseMmap(true);
                readers[i] = columns[i].getReader();
            }
            for (int i = 0; i < N; i++) {
                for (int c = 0; c < readers.length; c++)
                    readers[c].get(i);
            }
            t = System.currentTimeMillis() - t;
            System.out.println(N + " records read with mmap, " + calcSpeed(t) + "K rec/sec");
        }
    }

    private int calcSpeed(long t) {
        double sec = (double) t / 1000;
        return (int) (N / sec / 1000);
    }
}
