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

package io.kyligence.kap.common.constant;

import lombok.Getter;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public enum NonCustomProjectLevelConfig {
    // project setting
    RECOMMENDATION_AUTO_MODE("kylin.metadata.semi-automatic-mode"),

    STORAGE_QUOTA_SIZE("kylin.storage.quota-in-giga-bytes"),

    FREQUENCY_TIME_WINDOW_IN_DAYS("kylin.cube.frequency-time-window"),
    LOW_FREQUENCY_THRESHOLD("kylin.cube.low-frequency-threshold"),

    PUSH_DOWN_ENABLED("kylin.query.pushdown-enabled"),

    JOB_DATA_LOAD_EMPTY_NOTIFICATION_ENABLED("kylin.job.notification-on-empty-data-load"),
    JOB_ERROR_NOTIFICATION_ENABLED("kylin.job.notification-on-job-error"),
    NOTIFICATION_ADMIN_EMAILS("kylin.job.notification-admin-emails"),

    ENGINE_SPARK_YARN_QUEUE("kylin.engine.spark-conf.spark.yarn.queue"),

    MULTI_PARTITION_ENABLED("kylin.model.multi-partition-enabled"),

    SNAPSHOT_MANUAL_MANAGEMENT_ENABLED("kylin.snapshot.manual-management-enabled"),

    EXPOSE_COMPUTED_COLUMN("kylin.query.metadata.expose-computed-column"),

    QUERY_NON_EQUI_JOIN_MODEL_ENABLED("kylin.query.non-equi-join-model-enabled"),

    // extra
    DATASOURCE_TYPE("kylin.source.default");

    private final String value;

    NonCustomProjectLevelConfig(String value) {
        this.value = value;
    }

    public static Set<String> listAllConfigNames() {
        return Stream.of(NonCustomProjectLevelConfig.values())
                .map(NonCustomProjectLevelConfig::getValue)
                .collect(Collectors.toSet());
    }
}
