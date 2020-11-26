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

package io.kyligence.kap.rest.interceptor;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

public class ProjectInfoParserTest {
    private final String project = "test";
    private final String urlPrefix = "/kylin/api/projects/" + project;

    @Test
    public void testBasic() {


        String[] urls = new String[] {
                // project api
                urlPrefix,
                urlPrefix + "/backup",
                urlPrefix + "/default_database",
                urlPrefix + "/query_accelerate_threshold",
                urlPrefix + "/storage",
                urlPrefix + "/storage_quota",
                urlPrefix + "/shard_num_config",
                urlPrefix + "/garbage_cleanup_config",
                urlPrefix + "/job_notification_config",
                urlPrefix + "/push_down_config",
                urlPrefix + "/push_down_project_config",
                urlPrefix + "/computed_column_config",
                urlPrefix + "/segment_config",
                urlPrefix + "/project_general_info",
                urlPrefix + "/project_config",
                urlPrefix + "/source_type",
                urlPrefix + "/yarn_queue",
                urlPrefix + "/computed_column_config",
                urlPrefix + "/owner",
                urlPrefix + "/config",

                // other api
                "/kylin/api/models/" + project + "/model1/partition_desc"
        };

        for (String url : urls) {
            Assert.assertEquals(project, ProjectInfoParser.extractProject(url));
        }
    }

    @Test
    public void testFailed() {
        String wrongUrl = "/wrong_url";
        String[] urls = new String[] {
                urlPrefix + wrongUrl,
                urlPrefix + "/backup" + wrongUrl,
                urlPrefix + "/push_down_project_config" + wrongUrl,
                urlPrefix + "/config" + wrongUrl,
                //
                urlPrefix + wrongUrl + "/backup",
                urlPrefix + wrongUrl + "/push_down_project_config",
                urlPrefix + wrongUrl + "/config",
                //
                "/kylin/api/models/" + project + "/model1/partition_desc" + wrongUrl,
                "/kylin/api/models/" + project + wrongUrl + "/model1/partition_desc",
        };

        for (String url : urls) {
            Assert.assertNotEquals(project, ProjectInfoParser.extractProject(url));
        }
    }

    @Test
    public void testRequest() {
        // project in parameter
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServerName("localhost");
        request.setRequestURI("/kylin/api/test");
        request.setParameter("project", "AAA");

        Pair<String, HttpServletRequest> pair = ProjectInfoParser.parseProjectInfo(request);
        Assert.assertEquals(pair.getFirst(), "AAA");

        // project in body
        request = new MockHttpServletRequest();
        request.setServerName("localhost");
        request.setRequestURI("/kylin/api/test");
        String body = "{\"project\": \"BBB\"}";

        request.setContent(body.getBytes());

        request.setContentType("application/json");
        pair = ProjectInfoParser.parseProjectInfo(request);
        Assert.assertEquals(pair.getFirst(), "BBB");

        // delete request
        request = new MockHttpServletRequest();
        request.setServerName("localhost");
        request.setRequestURI("/kylin/api/projects/CCC");
        request.setMethod("DELETE");

        request.setContentType("application/json");
        pair = ProjectInfoParser.parseProjectInfo(request);
        Assert.assertEquals(pair.getFirst(), "CCC");

        // project is empty
        request = new MockHttpServletRequest();
        request.setServerName("localhost");
        request.setRequestURI("/kylin/api/test");

        request.setContentType("application/json");
        pair = ProjectInfoParser.parseProjectInfo(request);
        Assert.assertEquals(pair.getFirst(), "_global");
    }
}
