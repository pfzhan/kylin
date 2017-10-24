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

package io.kyligence.kap.source.hive.tablestats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import junit.framework.TestCase;

public class HiveTableExtSamplerTest extends TestCase {
    @Test
    public void testHiveSample() {
        String[] stringValues = { "I love China", "", "麒麟最牛逼啊", "USA", "what is your name", "USA", "yes, I like it",
                "true", "Dinner is perfect", "Not very good" };
        String[] decimalValues = { "1.232323232434", "3.23232323", "-1.3232", "434.223232", "232.22323" };
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 256);

        for (int i = 0; i < stringValues.length; i++) {
            sampler.samples(stringValues[i]);
        }

        List<Long> mapperRows = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            mapperRows.add((long) i);
        }

        sampler.setMapperRows(mapperRows);
        sampler.sync();
        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);

        assertEquals("麒麟最牛逼啊", sampler.getMax());
        assertEquals(0, sampler.getMinLenValue().length());
        assertEquals("麒麟最牛逼啊", sampler.getMaxLenValue());
        sampler.clean();

        sampler = new HiveTableExtSampler("decimal", 19);

        for (int i = 0; i < decimalValues.length; i++) {
            sampler.samples(decimalValues[i]);
        }

        sampler.sync();
        buf = sampler.code();
        buf.flip();
        sampler.decode(buf);

        assertEquals("434.223232", sampler.getMax());
        assertEquals(7, sampler.getMinLenValue().length());
        assertEquals("0", sampler.getNullCounter());
        sampler.clean();
    }

    @Test
    public void testPrecision() {
        String longString = "";
        for (int i = 0; i < 1200; i++) {
            longString += "K";
        }
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 1300);
        sampler.samples(longString);
        sampler.sync();
        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);
    }

    @Test
    public void testExceedPrecisionValues() {
        String[] stringValues = { "I love China", "麒麟最牛逼啊", "USA", "what is your name", "USA", "yes, I like it", "true",
                "Dinner is perfect", "Not very good", "KYLIN is the best Big Data Warehouse" };
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 3);

        for (int i = 0; i < stringValues.length; i++) {
            sampler.samples(stringValues[i]);
        }

        sampler.sync();
        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);

        assertEquals("KYLIN is the best Big Data Warehouse", sampler.getExceedPrecisionMaxLengthValue());
        assertEquals(8, sampler.getExceedPrecisionCount());

        sampler = new HiveTableExtSampler("varchar", 256);

        for (int i = 0; i < stringValues.length; i++) {
            sampler.samples(stringValues[i]);
        }

        sampler.sync();
        buf = sampler.code();
        buf.flip();
        sampler.decode(buf);

        assertEquals(null, sampler.getExceedPrecisionMaxLengthValue());
        assertEquals(0, sampler.getExceedPrecisionCount());
    }

    @Test
    public void testSampleRaw() {

        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 100);
        List<String> rawList = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            rawList.add(String.valueOf(i));
        }
        for (String element : rawList) {
            sampler.samples(element);
        }
        String value = sampler.getRawSampleValue("1");
        assertNotSame("", value);
    }

    @Test
    public void testDataSkew() {
        List<String> skewSamples = new ArrayList<>();
        int counter = 500000;
        for (int i = 0; i < counter; i++) {
            if (i > counter / 2)
                skewSamples.add(String.valueOf(0));
            else {
                String v = UUID.randomUUID().toString();
                for (int j = 0; j < 4; j++)
                    v = v + v;
                skewSamples.add(v);
            }
        }
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 800);

        for (String e : skewSamples) {
            sampler.samples(e);
        }
        sampler.sync();
        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);
        assertEquals(counter / 2 - 1, (long) sampler.getTopN().getTopNCounter().get("0"));
    }

    @Test
    public void testMerge() {
        List<HiveTableExtSampler> samplers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            HiveTableExtSampler s = new HiveTableExtSampler("varchar", 100);
            samplers.add(s);
        }

        for (int i = 0; i < 12345; i++) {
            for (HiveTableExtSampler s : samplers) {
                String value = UUID.randomUUID().toString();
                s.samples(value);
            }
        }

        HiveTableExtSampler finalSampler = new HiveTableExtSampler("varchar", 100);
        for (HiveTableExtSampler s : samplers) {
            s.sync();
            ByteBuffer buf = s.code();
            buf.flip();
            s.decode(buf);
            finalSampler.merge(s);
        }

        ByteBuffer buf = finalSampler.code();
        buf.flip();
        finalSampler.decode(buf);
        finalSampler.getTopN();
    }

    @Test
    public void testModelStats() {
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 100, 0, 3);

        String[][] sampleValues = { { "1", "2", "3" }, { "4", "5", "6" }, { "7", "8", "9" } };

        for (int i = 0; i < sampleValues.length; i++)
            sampler.samples(sampleValues[i]);

        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);
    }

    @Test
    public void testMutable() {
        LinkedList<HiveTableExtSampler.SimpleTopN.MutableLong> testList = new LinkedList<>();
        HiveTableExtSampler sampler = new HiveTableExtSampler("varchar", 100);
        HiveTableExtSampler.SimpleTopN topN = sampler.new SimpleTopN(10);
        HiveTableExtSampler.SimpleTopN.MutableLong first = topN.new MutableLong();
        HiveTableExtSampler.SimpleTopN.MutableLong second = topN.new MutableLong();
        HiveTableExtSampler.SimpleTopN.MutableLong third = topN.new MutableLong();
        first.increment(Integer.MAX_VALUE);
        first.increment();
        third.increment(first.getValue() * 2);
        third.setValue(third.getValue());

        testList.add(first);
        testList.add(second);
        testList.add(third);

        Collections.sort(testList);
        assertEquals(1, testList.pollLast().getValue());
    }
}
