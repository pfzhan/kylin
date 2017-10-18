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

import org.junit.Assert;
import org.junit.Test;

public class EscapeTransformerTest {

    @Test
    public void normalFNTest() {
        String originalSQL = "select { fn count(*) }, avg(sales) from tbl";
        String expectedSQL = "select count(*), avg(sales) from tbl";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void leftFNTest() {
        String originalSQL = "select { fn LEFT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, 1, 2) from KYLIN_SALES";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void rightFNTest() {
        String originalSQL = "select { fn RIGHT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, CHAR_LENGTH(LSTG_FORMAT_NAME) - 1, 2) from KYLIN_SALES";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lengthFNTest() {
        String originalSQL = "select {fn LENGTH('Happy')}";
        String expectedSQL = "select CHAR_LENGTH('Happy')";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);

        Assert.assertEquals(expectedSQL, transformedSQL);
    }
    
    @Test
    public void convertFNTest() {
        String originalSQL = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_VARCHAR)})} from KYLIN_SALES";
        String expectedSQL = "select CAST(PART_DT AS DATE), TRIM(leading CAST(PRICE AS VARCHAR)) from KYLIN_SALES";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);

        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lcaseFNTest() {
        String originalSQL = "select { fn LCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select LOWER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ucaseFNTest() {
        String originalSQL = "select { fn UCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select UPPER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void quotedStringTest() {
        String originalSQL = "select 'Hello World!', {fn LENGTH('12345 67890')}";
        String expectedSQL = "select 'Hello World!', CHAR_LENGTH('12345 67890')";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);

        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void spaceDelimitersTest() {
        String originalSQL = "select 'Hello World!',\r\n\t {fn\tLENGTH('12345 \r\n\t 67890')}\nlimit 1";
        String expectedSQL = "select 'Hello World!',\r\n\t CHAR_LENGTH('12345 \r\n\t 67890')\nlimit 1";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);

        Assert.assertEquals(expectedSQL, transformedSQL);
    }
    
    @Test
    public void escapeTSTest() {

        String originalSQL = "select {ts '2013-01-01 00:00:00'}";
        String expectedSQL = "select CAST('2013-01-01 00:00:00' AS TIMESTAMP)";

        EscapeTransformer transformer = new EscapeTransformer();
        String transformedSQL = transformer.transform(originalSQL, null, null);

        Assert.assertEquals(expectedSQL, transformedSQL);
    }
}
