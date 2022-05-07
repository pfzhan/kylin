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

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class TimedJsonStreamParserTest extends NLocalFileMetadataTestCase {
    private static final String jsonFilePath = "src/test/resources/message.json";
    private static final String dupKeyJsonFilePath = "src/test/resources/message_with_dup_key.json";
    private static final String className = "io.kyligence.kap.parser.TimedJsonStreamParser";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void testFlattenMessage() throws Exception {

        InputStream is = new FileInputStream(jsonFilePath);
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));

        AbstractDataParser parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());

        Map<String, Object> flatMap = parser.process(buffer);
        assertEquals(29, flatMap.size());
        assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        assertEquals(755703618762862600L, flatMap.get("id"));
        assertEquals(false, flatMap.get("isTruncated"));
        assertEquals("dejamos", flatMap.get("text"));
        assertEquals("", flatMap.get("contributorsIDs"));
        assertEquals(755703584084328400L, flatMap.get("mediaEntities__0_id"));
        assertEquals(150, flatMap.get("mediaEntities__0_sizes_0_width"));
        assertEquals(100, flatMap.get("mediaEntities__0_sizes_1_resize"));
        assertEquals(4853763947L, flatMap.get("user_id"));
        assertEquals("Noticias", flatMap.get("user_description"));
        assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        assertEquals(false, flatMap.get("user_isProtected"));

    }

    @Test
    public void testFlattenMessageWithDupKey() throws Exception {
        InputStream is = Files.newInputStream(Paths.get(dupKeyJsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));
        AbstractDataParser parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());
        Map<String, Object> flatMap = parser.process(buffer);
        assertEquals(31, flatMap.size());
        assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        assertEquals(755703618762862600L, flatMap.get("id"));
        assertEquals(false, flatMap.get("isTruncated"));
        assertEquals("dejamos", flatMap.get("text"));
        assertEquals("", flatMap.get("contributorsIDs"));
        assertEquals(755703584084328400L, flatMap.get("mediaEntities__0_id"));
        assertEquals(150, flatMap.get("mediaEntities__0_sizes_0_width"));
        assertEquals(100, flatMap.get("mediaEntities__0_sizes_1_resize"));
        assertEquals("Noticias", flatMap.get("user_description"));
        assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        assertEquals(false, flatMap.get("user_isProtected"));

        // assert dup key val
        assertEquals(123456, flatMap.get("user_id"));
        assertEquals(4853763947L, flatMap.get("user_id_1"));
        assertEquals(654321, flatMap.get("user_id_1_1"));
    }

    @Test
    public void testException() throws Exception {
        String text = "test";
        AbstractDataParser parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());

        thrown.expect(RuntimeException.class);
        parser.process(StandardCharsets.UTF_8.encode(text));
    }
}