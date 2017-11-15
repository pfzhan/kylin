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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class CognosParenthesesEscapeTest {

    @Test
    public void basicTest() {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String data = " from ((a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3) inner join c as cc on a.x1=cc.z1 ) join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String expected = " from a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3 inner join c as cc on a.x1=cc.z1  join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String transformed = escape.completion(data);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced1Test() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced2Test() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query02.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query02.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced3Test() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query03.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query03.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void proguardTest() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        Collection<File> files = FileUtils.listFiles(new File("../../kylin/kylin-it/src/test/resources"),
                new String[] {"sql"}, true);
        for (File f : files) {
            System.out.println("checking " + f.getAbsolutePath());
            String query = FileUtils.readFileToString(f, Charset.defaultCharset());
            String transformed = escape.completion(query);
            Assert.assertEquals(query, transformed);
        }
    }
}
