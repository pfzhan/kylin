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
package io.kyligence.kap.rest.constant;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.google.common.collect.ImmutableList;

import lombok.val;

public class ProjectInfoParserConstant {
    public static final ProjectInfoParserConstant INSTANCE = new ProjectInfoParserConstant();
    public final List<String> PROJECT_PARSER_URI_LIST;

    private ProjectInfoParserConstant() {
        ImmutableList.Builder<String> urisBuilder = ImmutableList.builder();
        constructUris(urisBuilder);
        PROJECT_PARSER_URI_LIST = urisBuilder.build();

    }

    private void constructUris(final ImmutableList.Builder<String> urisBuilder) {

        //  /kylin/api/projects/{project}/XXXX/
        {
            val projectsSubUris = Arrays.asList("backup", "default_database", "query_accelerate_threshold",
                    "storage_volume_info", "storage", "storage_quota", "favorite_rules", "statistics", "acceleration",
                    "shard_num_config", "garbage_cleanup_config", "job_notification_config", "push_down_config",
                    "scd2_config", "push_down_project_config", "snapshot_config", "computed_column_config",
                    "segment_config", "project_general_info", "project_config", "source_type", "yarn_queue",
                    "project_kerberos_info", "owner", "config", "jdbc_config");

            projectsSubUris.forEach(projectsSubUri -> {
                urisBuilder.add(format(Locale.ROOT, "/kylin/api/projects/{project}/%s", projectsSubUri));
            });

            urisBuilder.add("/kylin/api/projects/{project}");
        }

        {
            urisBuilder.add("/kylin/api/models/{project}/{model}/model_desc");
            urisBuilder.add("/kylin/api/models/{project}/{model}/partition_desc");
        }

        // application/vnd.apache.kylin-v2+json
        {
            urisBuilder.add("/api/access/{type}/{project}");
        }
    }
}
