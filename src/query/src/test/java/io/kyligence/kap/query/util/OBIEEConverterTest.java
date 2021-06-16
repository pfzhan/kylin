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
package io.kyligence.kap.query.util;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class OBIEEConverterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConvertDoubleQuoteSuccess() {
        assertConverted(
                "select * from a where b in (1.0, 1.03E+08, 123.000, 0.00, -1.20, -0.02, -132.00)",
                "select * from a where b in (1, 1.03E+08, 123, 0, -1.20, -0.02, -132)");
        assertConverted(
                "select * from a where a  = 1.0 or b = 123 and c in (1.0, 11) and c = '1.0'",
                "select * from a where a  = 1 or b = 123 and c in (1.0, 11) and c = '1.0'");
        assertConverted(
                "select * from a inner join b where a = 1.0 and b = 2.0",
                "select * from a inner join b where a = 1 and b = 2");
        assertConverted(
                "select * from a where a  = 1.0 union select * from a where a  = 1.0 or b = 123 and c in (1.0, 11)",
                "select * from a where a  = 1 union select * from a where a  = 1.0 or b = 123 and c in (1.0, 11)");
        assertConverted(
                "select *, 1.0 from a where a  = 1.0 group by c",
                "select *, 1.0 from a where a  = 1 group by c");
    }

    public void assertConverted(String original, String expected) {
        Assert.assertEquals(expected, new OBIEEConverter().convert(original, "default", "default"));
    }
}
