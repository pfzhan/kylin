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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.measure.topn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Ignore("For collecting accuracy statistics, not for functional test")
public class TopNCounterTest {

    protected static int TOP_K;

    protected static int KEY_SPACE;

    protected static int TOTAL_RECORDS;

    protected static int SPACE_SAVING_ROOM;

    protected static int PARALLEL = 10;

    protected static boolean verbose = true;

    public TopNCounterTest() {
        TOP_K = 100;
        KEY_SPACE = 100 * TOP_K;
        TOTAL_RECORDS = 1000000; // 1 million
        SPACE_SAVING_ROOM = 100;
    }

    protected String prepareTestDate() throws IOException {
        String[] allKeys = new String[KEY_SPACE];

        for (int i = 0; i < KEY_SPACE; i++) {
            allKeys[i] = RandomStringUtils.randomAlphabetic(10);
        }

        outputMsg("Start to create test random data...");
        long startTime = System.currentTimeMillis();
        ZipfDistribution zipf = new ZipfDistribution(KEY_SPACE, 0.5);
        int keyIndex;

        File tempFile = File.createTempFile("ZipfDistribution", ".txt");

        if (tempFile.exists())
            FileUtils.forceDelete(tempFile);
        FileWriter fw = new FileWriter(tempFile);
        try {
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                keyIndex = zipf.sample() - 1;
                fw.write(allKeys[keyIndex]);
                fw.write('\n');
            }
        } finally {
            if (fw != null)
                fw.close();
        }

        outputMsg("Create test data takes : " + (System.currentTimeMillis() - startTime) / 1000 + " seconds.");
        outputMsg("Test data in : " + tempFile.getAbsolutePath());

        return tempFile.getAbsolutePath();
    }

    //@Test
    public void testSingleSpaceSaving() throws IOException {
        String dataFile = prepareTestDate();
        TopNCounterTest.SpaceSavingConsumer spaceSavingCounter = new TopNCounterTest.SpaceSavingConsumer(
                TOP_K * SPACE_SAVING_ROOM);
        TopNCounterTest.HashMapConsumer accurateCounter = new TopNCounterTest.HashMapConsumer();

        for (TopNCounterTest.TestDataConsumer consumer : new TopNCounterTest.TestDataConsumer[] { spaceSavingCounter,
                accurateCounter }) {
            feedDataToConsumer(dataFile, consumer, 0, TOTAL_RECORDS);
        }

        FileUtils.forceDelete(new File(dataFile));

        compareResult(spaceSavingCounter, accurateCounter);
    }

    private void compareResult(TopNCounterTest.TestDataConsumer firstConsumer,
            TopNCounterTest.TestDataConsumer secondConsumer) {
        List<Pair<String, Double>> topResult1 = firstConsumer.getTopN(TOP_K);
        outputMsg("Get topN, Space saving takes " + firstConsumer.getSpentTime() / 1000 + " seconds");
        List<Pair<String, Double>> realSequence = secondConsumer.getTopN(TOP_K);
        outputMsg("Get topN, Merge sort takes " + secondConsumer.getSpentTime() / 1000 + " seconds");

        int error = 0;
        for (int i = 0; i < topResult1.size(); i++) {
            outputMsg("Compare " + i);

            if (isClose(topResult1.get(i).getSecond().doubleValue(), realSequence.get(i).getSecond().doubleValue())) {
                //            if (topResult1.get(i).getFirst().equals(realSequence.get(i).getFirst()) && topResult1.get(i).getSecond().doubleValue() == realSequence.get(i).getSecond().doubleValue()) {
                outputMsg("Passed; key:" + topResult1.get(i).getFirst() + ", value:" + topResult1.get(i).getSecond());
            } else {
                outputMsg("Failed; space saving key:" + topResult1.get(i).getFirst() + ", value:"
                        + topResult1.get(i).getSecond());
                outputMsg("Failed; correct key:" + realSequence.get(i).getFirst() + ", value:"
                        + realSequence.get(i).getSecond());
                error++;
            }
        }

        org.junit.Assert.assertEquals(0, error);
    }

    private boolean isClose(double value1, double value2) {

        if (Math.abs(value1 - value2) < 5.0)
            return true;

        return false;
    }

    @Test
    public void testParallelSpaceSaving() throws IOException, ClassNotFoundException {
        String dataFile = prepareTestDate();

        TopNCounterTest.SpaceSavingConsumer[] parallelCounters = new TopNCounterTest.SpaceSavingConsumer[PARALLEL];

        for (int i = 0; i < PARALLEL; i++) {
            parallelCounters[i] = new TopNCounterTest.SpaceSavingConsumer(TOP_K * SPACE_SAVING_ROOM);
        }

        int slice = TOTAL_RECORDS / PARALLEL;
        int startPosition = 0;
        for (int i = 0; i < PARALLEL; i++) {
            feedDataToConsumer(dataFile, parallelCounters[i], startPosition, startPosition + slice);
            startPosition += slice;
        }

        TopNCounterTest.SpaceSavingConsumer[] mergedCounters = singleMerge(parallelCounters);

        TopNCounterTest.HashMapConsumer accurateCounter = new TopNCounterTest.HashMapConsumer();
        feedDataToConsumer(dataFile, accurateCounter, 0, TOTAL_RECORDS);

        compareResult(mergedCounters[0], accurateCounter);
        FileUtils.forceDelete(new File(dataFile));

    }

    @Test
    public void testComparator() {
        List<Counter> counters = Lists.newArrayList(new Counter<>("item1", 1d), new Counter<>("item1", 2d),
                new Counter<>("item1", 3d), new Counter<>("item1", null), new Counter<>("item1", null),
                new Counter<>("item1", 1d), new Counter<>("item1", 3d), new Counter<>("item2", 1d),
                new Counter<>("item2", 3d));
        counters.sort(TopNCounter.ASC_COMPARATOR);

        List<Double> expectedCounts = Lists.newArrayList(null, null, 1d, 1d, 1d, 2d, 3d, 3d, 3d);
        List<Double> originCounts = Lists.newArrayList();
        counters.stream().forEach(counter -> {
            originCounts.add(counter.getCount());
        });
        Assert.assertArrayEquals(expectedCounts.toArray(), originCounts.toArray());

        counters.sort(TopNCounter.DESC_COMPARATOR);
        List<Double> expectedDescCounts = Lists.newArrayList(3d, 3d, 3d, 2d, 1d, 1d, 1d, null, null);
        List<Double> originDescCounts = Lists.newArrayList();
        counters.stream().forEach(counter -> {
            originDescCounts.add(counter.getCount());
        });
        Assert.assertArrayEquals(expectedDescCounts.toArray(), originDescCounts.toArray());
    }

    /**
     * https://github.com/Kyligence/KAP/issues/16933
     *
     * the error of “Comparison method violates its general contract!”
     * are deep in the timsort algorithm and there are two necessary
     * and insufficient conditions to reproduce this problem.
     *
     * 1.the size of list is greater than 32.
     * 2.there must be at least two runs in the list.
     */
    @Test
    public void testComparatorSymmetry() {
        List<Counter> counters = Lists.newArrayList(new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 3d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 1d), new Counter<>("item", 1d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 1d),
                new Counter<>("item", 0d), new Counter<>("item", 1d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 1d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 1d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 0d), new Counter<>("item", 2d), new Counter<>("item", 1d),
                new Counter<>("item", 0d), new Counter<>("item", 0d), new Counter<>("item", 0d),
                new Counter<>("item", 2d), new Counter<>("item", 4d), new Counter<>("item", 0d),
                new Counter<>("item", 3d));
        counters.sort(TopNCounter.ASC_COMPARATOR);
        List<Double> expectedCounts = Lists.newArrayList(0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
                0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
                0d, 0d, 0d, 0d, 0d, 0d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 2d, 2d, 3d, 3d, 4d);
        List<Double> originCounts = Lists.newArrayList();
        counters.stream().forEach(counter -> {
            originCounts.add(counter.getCount());
        });
        Assert.assertArrayEquals(expectedCounts.toArray(), originCounts.toArray());

        counters.sort(TopNCounter.DESC_COMPARATOR);
        List<Double> expectedDescCounts = Lists.newArrayList(4d, 3d, 3d, 2d, 2d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 0d, 0d, 0d,
                0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
                0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d);
        List<Double> originDescCounts = Lists.newArrayList();
        counters.stream().forEach(counter -> {
            originDescCounts.add(counter.getCount());
        });
        Assert.assertArrayEquals(expectedDescCounts.toArray(), originDescCounts.toArray());
    }

    private TopNCounterTest.SpaceSavingConsumer[] singleMerge(TopNCounterTest.SpaceSavingConsumer[] consumers)
            throws IOException, ClassNotFoundException {
        List<TopNCounterTest.SpaceSavingConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        TopNCounterTest.SpaceSavingConsumer merged = new TopNCounterTest.SpaceSavingConsumer(TOP_K * SPACE_SAVING_ROOM);

        for (int i = 0, n = consumers.length; i < n; i++) {
            merged.vs.merge(consumers[i].vs);
        }

        merged.vs.retain(TOP_K * SPACE_SAVING_ROOM); // remove extra elements;
        return new TopNCounterTest.SpaceSavingConsumer[] { merged };

    }

    private TopNCounterTest.SpaceSavingConsumer[] binaryMerge(TopNCounterTest.SpaceSavingConsumer[] consumers)
            throws IOException, ClassNotFoundException {
        List<TopNCounterTest.SpaceSavingConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        for (int i = 0, n = consumers.length; i < n; i = i + 2) {
            if (i + 1 < n) {
                consumers[i].vs.merge(consumers[i + 1].vs);
            }

            list.add(consumers[i]);
        }

        return binaryMerge(list.toArray(new TopNCounterTest.SpaceSavingConsumer[list.size()]));
    }

    private void feedDataToConsumer(String dataFile, TopNCounterTest.TestDataConsumer consumer, int startLine,
            int endLine) throws IOException {
        long startTime = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile));

        int lineNum = 0;
        String line = bufferedReader.readLine();
        while (line != null) {
            if (lineNum >= startLine && lineNum < endLine) {
                consumer.addElement(line, 1.0);
            }
            line = bufferedReader.readLine();
            lineNum++;
        }

        bufferedReader.close();
        outputMsg("feed data to " + consumer.getClass().getCanonicalName() + " take time "
                + (System.currentTimeMillis() - startTime) / 1000 + "s");
    }

    private void outputMsg(String msg) {
        if (verbose)
            System.out.println(msg);
    }

    private static interface TestDataConsumer {
        public void addElement(String elementKey, double value);

        public List<Pair<String, Double>> getTopN(int k);

        public long getSpentTime();
    }

    private class SpaceSavingConsumer implements TopNCounterTest.TestDataConsumer {
        private long timeSpent = 0;
        protected TopNCounter<String> vs;

        public SpaceSavingConsumer(int space) {
            vs = new TopNCounter<String>(space);

        }

        public void addElement(String key, double value) {
            //outputMsg("Adding " + key + ":" + incrementCount);
            long startTime = System.currentTimeMillis();
            vs.offer(key, value);
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Counter<String>> tops = vs.topK(k);
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Counter<String> counter : tops)
                allRecords.add(Pair.newPair(counter.getItem(), counter.getCount()));
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords;
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }
    }

    private class HashMapConsumer implements TopNCounterTest.TestDataConsumer {

        private long timeSpent = 0;
        private Map<String, Double> hashMap;

        public HashMapConsumer() {
            hashMap = Maps.newHashMap();
        }

        public void addElement(String key, double value) {
            long startTime = System.currentTimeMillis();
            if (hashMap.containsKey(key)) {
                hashMap.put(key, hashMap.get(key) + value);
            } else {
                hashMap.put(key, value);
            }
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Map.Entry<String, Double> entry : hashMap.entrySet()) {
                allRecords.add(Pair.newPair(entry.getKey(), entry.getValue()));
            }

            Collections.sort(allRecords, new Comparator<Pair<String, Double>>() {
                @Override
                public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
                    return o1.getSecond() < o2.getSecond() ? 1 : (o1.getSecond() > o2.getSecond() ? -1 : 0);
                }
            });
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords.subList(0, k);
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }
    }

}
