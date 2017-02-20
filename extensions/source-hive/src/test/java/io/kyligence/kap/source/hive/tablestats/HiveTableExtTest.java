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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import junit.framework.TestCase;

public class HiveTableExtTest extends TestCase {
    @Test
    public void testHiveSample() {
        String[] stringValues = { "I love China", "", "麒麟最牛逼啊", "USA", "what is your name", "yes, I like it", "true", "Dinner is perfect", "Not very good" };
        String[] decimalValues = { "1.232323232434", "3.23232323", "-1.3232", "434.223232", "232.22323" };
        HiveTableExtSampler sampler = new HiveTableExtSampler();
        sampler.setDataType("varchar");

        for (int i = 0; i < stringValues.length; i++) {
            sampler.samples(stringValues[i], 1);
        }
        sampler.sync(0);
        sampler.code();
        sampler.getBuffer().flip();
        sampler.decode(sampler.getBuffer());

        assertEquals("麒麟最牛逼啊", sampler.getMax());
        assertEquals(0, sampler.getMinLenValue().length());
        assertEquals("麒麟最牛逼啊", sampler.getMaxLenValue());
        sampler.clean();

        sampler = new HiveTableExtSampler();
        sampler.setDataType("decimal");

        for (int i = 0; i < decimalValues.length; i++) {
            sampler.samples(decimalValues[i], 0);
        }

        sampler.sync(0);
        sampler.code();
        sampler.getBuffer().flip();
        sampler.decode(sampler.getBuffer());

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
        int counter = 0;
        for (String element : rawList) {
            sampler.samples(element, counter);
            counter++;
        }
        String value = sampler.getRawSampleValue("1");
        assertNotSame("", value);
    }
}
