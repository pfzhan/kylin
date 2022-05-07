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

package io.kyligence.kap.metadata.streaming;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_PARSER;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;

public class DataParserManagerTest extends NLocalFileMetadataTestCase {

    private DataParserManager manager;
    private static final String project = "streaming_test";
    private static final String defaultClassName = "io.kyligence.kap.parser.TimedJsonStreamParser";
    private static final String jarPath = "default";
    private static final String test = "test";
    private static final String usingTable = "DEFAULT.SSB_STREAMING";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        manager = DataParserManager.getInstance(getTestConfig(), project);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testInitDefault() {
        manager.initDefault(project);

        manager = DataParserManager.getInstance(getTestConfig(), "default");
        manager.initDefault("default");
    }

    @Test
    public void testGetDataParserInfo() {

        DataParserInfo dataParserInfo1 = manager.getDataParserInfo(defaultClassName);
        Assert.assertNotNull(dataParserInfo1);
        Assert.assertEquals(project, dataParserInfo1.getProject());
        Assert.assertEquals(defaultClassName, dataParserInfo1.getClassName());
        Assert.assertEquals(jarPath, dataParserInfo1.getJarPath());
        Assert.assertNotNull(dataParserInfo1.getStreamingTables().get(0));

        DataParserInfo dataParserInfo2 = manager.getDataParserInfo(null);
        Assert.assertNull(dataParserInfo2);
    }

    @Test
    public void testCreateDataParserInfoNull() {
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createDataParserInfo(null));
    }

    @Test
    public void testCreateDataParserInfoEmptyClass() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, "", jarPath);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createDataParserInfo(dataParserInfo));
    }

    @Test
    public void testCreateDataParserInfoContains() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, defaultClassName, jarPath);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createDataParserInfo(dataParserInfo));
    }

    @Test
    public void testUpdateDataParserInfoException() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, test, jarPath);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.updateDataParserInfo(dataParserInfo));

    }

    @Test
    public void testUpdateDataParserInfo() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, test, jarPath);
        manager.createDataParserInfo(dataParserInfo);
        List<String> streamingTables = dataParserInfo.getStreamingTables();
        int except = streamingTables.size();
        streamingTables.add(test);
        dataParserInfo.setStreamingTables(streamingTables);
        manager.updateDataParserInfo(dataParserInfo);

        DataParserInfo dataParserInfo1 = manager.getDataParserInfo(test);
        Assert.assertEquals(except + 1, dataParserInfo1.getStreamingTables().size());
    }

    @Test
    public void testRemoveParserException() {

        // remove have tables
        DataParserInfo dataParserInfo2 = manager.removeParser(test);
        Assert.assertNull(dataParserInfo2);

        // remove normal
        DataParserInfo dataParserInfo3 = new DataParserInfo(project, test, test);
        manager.createDataParserInfo(dataParserInfo3);
        DataParserInfo dataParserInfo4 = manager.removeParser(test);
        Assert.assertNotNull(dataParserInfo4);

        // exception
        DataParserInfo dataParserInfo5 = new DataParserInfo(project, test, test);
        dataParserInfo5.getStreamingTables().add(test);
        manager.createDataParserInfo(dataParserInfo5);
        Assert.assertThrows(
                CUSTOM_PARSER_TABLES_USE_PARSER.getMsg(StringUtils.join(dataParserInfo5.getStreamingTables(), ",")),
                KylinException.class, () -> manager.removeParser(test));
    }

    @Test
    public void testRemoveDefaultParser() {
        // remove default
        Assert.assertThrows(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER.getMsg(), KylinException.class,
                () -> manager.removeParser(defaultClassName));
    }

    @Test
    public void testRemoveJar() {
        DataParserInfo dataParserInfo1 = new DataParserInfo(project, test, test);
        manager.createDataParserInfo(dataParserInfo1);
        manager.removeJar(test);
        DataParserInfo dataParserInfo2 = manager.getDataParserInfo(test);
        Assert.assertNull(dataParserInfo2);

        DataParserInfo dataParserInfo3 = new DataParserInfo(project, test, test);
        dataParserInfo3.setStreamingTables(Lists.newArrayList("DEFAULT.SSB_STREAMING"));
        manager.createDataParserInfo(dataParserInfo3);
        Assert.assertThrows(
                CUSTOM_PARSER_TABLES_USE_JAR.getMsg(StringUtils.join(manager.getJarParserUseTable(test), ",")),
                KylinException.class, () -> manager.removeJar(test));
    }

    @Test
    public void testRemoveDefaultJar() {
        Assert.assertThrows(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER.getMsg(), KylinException.class,
                () -> manager.removeJar("default"));
    }

    @Test
    public void testRemoveUsingTable() {
        DataParserInfo dataParserInfo = manager.removeUsingTable(usingTable, defaultClassName);
        Assert.assertNotNull(dataParserInfo);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.removeUsingTable(usingTable, test));

    }

    @Test
    public void testListDataParserInfo() {
        List<DataParserInfo> list = manager.listDataParserInfo();
        Assert.assertTrue(list.size() > 0);
    }

}
