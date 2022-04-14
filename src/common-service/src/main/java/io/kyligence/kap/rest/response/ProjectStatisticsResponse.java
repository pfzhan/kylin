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

package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProjectStatisticsResponse {
    @JsonProperty("database_size")
    private int databaseSize;
    @JsonProperty("table_size")
    private int tableSize;
    @JsonProperty("last_week_query_count")
    private long lastWeekQueryCount;
    @JsonProperty("unhandled_query_count")
    private long unhandledQueryCount;
    @JsonProperty("additional_rec_pattern_count")
    private int additionalRecPatternCount;
    @JsonProperty("removal_rec_pattern_count")
    private int removalRecPatternCount;
    @JsonProperty("rec_pattern_count")
    private int recPatternCount;
    @JsonProperty("effective_rule_size")
    private int effectiveRuleSize;
    @JsonProperty("approved_rec_count")
    private int approvedRecCount;
    @JsonProperty("approved_additional_rec_count")
    private int approvedAdditionalRecCount;
    @JsonProperty("approved_removal_rec_count")
    private int approvedRemovalRecCount;
    @JsonProperty("model_size")
    private int modelSize;
    @JsonProperty("acceptable_rec_size")
    private int acceptableRecSize;
    @JsonProperty("is_refreshed")
    private boolean refreshed;
    @JsonProperty("max_rec_show_size")
    private int maxRecShowSize;
}
