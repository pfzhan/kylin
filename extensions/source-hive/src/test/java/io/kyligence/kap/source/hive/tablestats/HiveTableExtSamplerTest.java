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
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import junit.framework.TestCase;

public class HiveTableExtSamplerTest extends TestCase {
    @Test
    public void testHiveSample() {
        String[] stringValues = { "I love China", "", "麒麟最牛逼啊", "USA", "what is your name", "USA", "yes, I like it", "true", "Dinner is perfect", "Not very good" };
        String[] decimalValues = { "1.232323232434", "3.23232323", "-1.3232", "434.223232", "232.22323" };
        HiveTableExtSampler sampler = new HiveTableExtSampler();
        sampler.setDataType("varchar");

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

        sampler = new HiveTableExtSampler();
        sampler.setDataType("decimal");

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
    public void testSampleRaw() {

        HiveTableExtSampler sampler = new HiveTableExtSampler();
        sampler.setDataType("varchar");
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
        int counter = 5000000;
        for (int i = 0; i < counter; i++) {
            if (i > counter / 2)
                skewSamples.add(String.valueOf(0));
            else {
                String v = UUID.randomUUID().toString();
                skewSamples.add(v);
            }
        }
        HiveTableExtSampler sampler = new HiveTableExtSampler();
        sampler.setDataType("varchar");

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
    public void testModelStats() {
        HiveTableExtSampler sampler = new HiveTableExtSampler(0, 3);
        sampler.setDataType("varchar");

        String[][] sampleValues = { { "1", "2", "3" }, { "4", "5", "6" }, { "7", "8", "9" } };

        for (int i = 0; i < sampleValues.length; i++)
            sampler.samples(sampleValues[i]);

        ByteBuffer buf = sampler.code();
        buf.flip();
        sampler.decode(buf);
    }
}
