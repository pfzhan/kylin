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

package io.kyligence.kap.parser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.exception.KylinException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Maps;

import lombok.val;

public class AbstractDataParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static String defaultClassName = "io.kyligence.kap.parser.TimedJsonStreamParser";

    @Test
    public void testGetDataParser() throws Exception {
        String json1 = "{\"cust_no\":\"343242\",\"windowDate\":\"2021-06-01 00:00:00\","
                + "\"windowDateLong\":\"1625037465002\",\"msg_type\":\"single\",\"msg_type1\":\"old\","
                + "\"msg_type2\":\"jily\",\"msg_type3\":\"pandora\",\"age\":\"32\",\"bal1\":21,\"bal2\":12,\"bal3\":13,\"bal4\":15,\"bal5\":22}";
        String json2 = "{\"cust_no\":\"343242\",\"windowDate\":\"2021-06-01 00:00:00\","
                + "\"windowDateLong\":\"1625037465002\",\"msg_type\":\"single\",\"msg_type1\":\"old\","
                + "\"msg_type2\":\"jily\",\"msg_type3\":\"pandora\",\"age\":\"32\",\"bal1\":21,\"bal2\":12,\"bal3\":13,\"bal4\":15,\"bal5\":22";
        val dataParser = AbstractDataParser.getDataParser(defaultClassName,
                Thread.currentThread().getContextClassLoader());
        Assert.assertNotNull(dataParser);
        dataParser.before();
        dataParser.after(Maps.newHashMap());
        Assert.assertTrue(dataParser.defineDataTypes().isEmpty());
        dataParser.process(StandardCharsets.UTF_8.encode(json1));
        dataParser.process(null);
        ByteBuffer encode = StandardCharsets.UTF_8.encode(json2);
        Assert.assertThrows(KylinException.class, () -> dataParser.process(encode));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertThrows(IllegalStateException.class,
                () -> AbstractDataParser.getDataParser(null, contextClassLoader));
        Assert.assertThrows(IllegalStateException.class,
                () -> AbstractDataParser.getDataParser("java.lang.String", contextClassLoader));
    }
}
