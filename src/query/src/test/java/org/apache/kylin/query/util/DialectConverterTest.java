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

package org.apache.kylin.query.util;

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class DialectConverterTest extends NLocalFileMetadataTestCase {

    DialectConverter dialectConverter = new DialectConverter();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConvertSuccess() {
        List<Pair<String, String>> convertList = Lists.newArrayList(
                Pair.newPair("select PRICE from KYLIN_SALES \n limit 1", "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROWS ONLY ",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROW ONLY ",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROWS\n FETCH NEXT 1 ROWS ONLY ", "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROWS", "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROW", "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH NEXT 1 ROWS ONLY", "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1")
        );

        for (Pair<String, String> p : convertList) {
            Assert.assertEquals(dialectConverter.convert(p.getFirst(), null, null), p.getSecond());
        }
    }

    @Test
    public void testConvertFailure() {
        List<String> convertList = Lists
                .newArrayList("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROWS  ONLY limit 1");

        for (String s : convertList) {
            Assert.assertEquals(dialectConverter.convert(s, null, null), s);
        }
    }
}