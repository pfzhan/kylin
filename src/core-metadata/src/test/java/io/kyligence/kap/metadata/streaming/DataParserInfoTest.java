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

import org.junit.Assert;
import org.junit.Test;

public class DataParserInfoTest {

    @Test
    public void testCreate() {
        {
            String project = "streaming_test";
            String defaultClassName = "io.kyligence.kap.parser.TimedJsonStreamParser";
            String jarPath = "default";
            DataParserInfo dataParserInfo = new DataParserInfo(project, defaultClassName, jarPath);
            dataParserInfo.getStreamingTables().add("table1");
            Assert.assertEquals(project, dataParserInfo.getProject());
            Assert.assertEquals(defaultClassName, dataParserInfo.getClassName());
            Assert.assertEquals(jarPath, dataParserInfo.getJarPath());
            Assert.assertNotNull(dataParserInfo.getStreamingTables().get(0));
            Assert.assertEquals(defaultClassName, dataParserInfo.resourceName());
            Assert.assertEquals("/streaming_test/parser/io.kyligence.kap.parser.TimedJsonStreamParser.json",
                    dataParserInfo.getResourcePath());
        }

        {
            DataParserInfo dataParserInfo = new DataParserInfo();
            Assert.assertNull(dataParserInfo.getProject());
            Assert.assertNull(dataParserInfo.getClassName());
            Assert.assertNull(dataParserInfo.getJarPath());
            Assert.assertEquals(0, dataParserInfo.getStreamingTables().size());
        }
    }

}
