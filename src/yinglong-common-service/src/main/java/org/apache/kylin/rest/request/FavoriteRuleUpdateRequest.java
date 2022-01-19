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

package org.apache.kylin.rest.request;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class FavoriteRuleUpdateRequest implements Serializable, ProjectInsensitiveRequest {

    private String project;

    @Deprecated
    @JsonProperty("freq_enable")
    private boolean freqEnable;
    @Deprecated
    @JsonProperty("freq_value")
    private String freqValue;

    @JsonProperty("count_enable")
    private boolean countEnable;
    @JsonProperty("count_value")
    private String countValue;
    @JsonProperty("duration_enable")
    private boolean durationEnable;
    @JsonProperty("min_duration")
    private String minDuration;
    @JsonProperty("max_duration")
    private String maxDuration;
    @JsonProperty("submitter_enable")
    private boolean submitterEnable;
    private List<String> users;
    @JsonProperty("user_groups")
    private List<String> userGroups;
    @JsonProperty("recommendation_enable")
    private boolean recommendationEnable;
    @JsonProperty("recommendations_value")
    private String recommendationsValue;
    @JsonProperty("excluded_tables_enable")
    private boolean excludeTablesEnable;
    @JsonProperty("excluded_tables")
    private String excludedTables;
    @JsonProperty("min_hit_count")
    private String minHitCount;
    @JsonProperty("effective_days")
    private String effectiveDays;
    @JsonProperty("update_frequency")
    private String updateFrequency;
}
