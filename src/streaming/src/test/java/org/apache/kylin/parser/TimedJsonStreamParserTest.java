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

package org.apache.kylin.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.exceptions.JsonParseException;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TimedJsonStreamParserTest {
    private static final String jsonFilePath = "src/test/resources/message.json";
    private static final String dupKeyJsonFilePath = "src/test/resources/message_with_dup_key.json";
    private static final String className = "io.kyligence.kap.parser.TimedJsonStreamParser";

    @Test
    public void testFlattenMessage() throws Exception {

        InputStream is = Files.newInputStream(Paths.get(jsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));

        AbstractDataParser<ByteBuffer, Map<String, Object>> parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());
        Optional<Map<String, Object>> flatMapOpt = parser.process(buffer);
        Assert.assertTrue(flatMapOpt.isPresent());
        Map<String, Object> flatMap = flatMapOpt.get();
        Assert.assertEquals(31, flatMap.size());
        Assert.assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        Assert.assertEquals(755703618762862600L, flatMap.get("id"));
        Assert.assertEquals(false, flatMap.get("isTruncated"));
        Assert.assertEquals("dejamos", flatMap.get("text"));
        Assert.assertEquals("", flatMap.get("contributorsIDs"));
        Assert.assertEquals(755703584084328400L, flatMap.get("mediaEntities_0_id"));
        Assert.assertEquals(150, flatMap.get("mediaEntities_0_sizes_0_width"));
        Assert.assertEquals(100, flatMap.get("mediaEntities_0_sizes_1_resize"));
        Assert.assertEquals(4853763947L, flatMap.get("user_id"));
        Assert.assertEquals("Noticias", flatMap.get("user_description"));
        Assert.assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        Assert.assertEquals(false, flatMap.get("user_isProtected"));
        Assert.assertNull(flatMap.get("clue_source_2_name"));
    }

    @Test
    public void testFlattenMessageWithDupKey() throws Exception {
        InputStream is = Files.newInputStream(Paths.get(dupKeyJsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));
        AbstractDataParser<ByteBuffer, Map<String, Object>> parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());
        Optional<Map<String, Object>> flatMapOpt = parser.process(buffer);
        Assert.assertTrue(flatMapOpt.isPresent());
        Map<String, Object> flatMap = flatMapOpt.get();
        Assert.assertEquals(31, flatMap.size());
        Assert.assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        Assert.assertEquals(755703618762862600L, flatMap.get("id"));
        Assert.assertEquals(false, flatMap.get("isTruncated"));
        Assert.assertEquals("dejamos", flatMap.get("text"));
        Assert.assertEquals("", flatMap.get("contributorsIDs"));
        Assert.assertEquals(755703584084328400L, flatMap.get("mediaEntities_0_id"));
        Assert.assertEquals(150, flatMap.get("mediaEntities_0_sizes_0_width"));
        Assert.assertEquals(100, flatMap.get("mediaEntities_0_sizes_1_resize"));
        Assert.assertEquals("Noticias", flatMap.get("user_description"));
        Assert.assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        Assert.assertEquals(false, flatMap.get("user_isProtected"));

        // assert dup key val
        Assert.assertEquals(123456, flatMap.get("user_id"));
        Assert.assertEquals(4853763947L, flatMap.get("user_id_1"));
        Assert.assertEquals(654321, flatMap.get("user_id_1_1"));
    }

    @Test
    public void testException() throws Exception {
        String text = "test";
        AbstractDataParser<ByteBuffer, Map<String, Object>> parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());
        ByteBuffer input = StandardCharsets.UTF_8.encode(text);
        Assert.assertThrows(JsonParseException.class, () -> parser.process(input));
    }

    @Test
    public void testParserConfig() throws IOException, ReflectiveOperationException {
        InputStream is = Files.newInputStream(Paths.get(jsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));

        AbstractDataParser<ByteBuffer, Map<String, Object>> parser = AbstractDataParser.getDataParser(className,
                Thread.currentThread().getContextClassLoader());
        ParserConfig config = new ParserConfig(true).setIncludes(Sets.newHashSet("isTruncated"));
        parser.withConfig(config);
        Optional<Map<String, Object>> flatMapOpt = parser.process(buffer);
        Assert.assertTrue(flatMapOpt.isPresent());
        Map<String, Object> flatMap = flatMapOpt.get();
        Assert.assertTrue(flatMap.containsKey("isTruncated"));

        config.setIncludes(Sets.newHashSet("istruncated"));
        parser.withConfig(config);
        flatMapOpt = parser.process(buffer);
        Assert.assertTrue(flatMapOpt.isPresent());
        flatMap = flatMapOpt.get();
        Assert.assertFalse(flatMap.containsKey("isTruncated"));

        config = new ParserConfig(false).setIncludes(Sets.newHashSet("istruncated"));
        parser.withConfig(config);
        flatMapOpt = parser.process(buffer);
        Assert.assertTrue(flatMapOpt.isPresent());
        flatMap = flatMapOpt.get();
        Assert.assertTrue(flatMap.containsKey("isTruncated"));
    }
}
