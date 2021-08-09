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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import lombok.Data;

@Data
public class OptRecLayoutResponse {
    @JsonProperty("item_id")
    private int id;
    @JsonProperty("is_agg")
    private boolean isAgg;
    @JsonProperty("is_add")
    private boolean isAdd;
    @JsonProperty("type")
    private RawRecItem.IndexRecType type;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("usage")
    private int usage;
    @JsonProperty("index_id")
    private long indexId;
    @JsonProperty("data_size")
    private long dataSize;
    @JsonProperty("memo_info")
    private Map<String, String> memoInfo = Maps.newHashMap();
    @JsonProperty("rec_detail_response")
    private OptRecDetailResponse recDetailResponse;
}
