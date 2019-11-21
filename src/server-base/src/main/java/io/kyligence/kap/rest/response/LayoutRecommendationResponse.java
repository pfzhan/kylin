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

import lombok.Data;

@Data
public class LayoutRecommendationResponse {
    @JsonProperty("id")
    private long id;

    @JsonProperty("item_id")
    private long itemId;

    @JsonProperty("data_size")
    private long dataSize;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("created_time")
    private long createdTime;

    @JsonProperty("columns_and_measures_size")
    private int columnsAndMeasuresSize;

    @JsonProperty("info")
    private Map<String, String> info = Maps.newHashMap();

    @JsonProperty("type")
    private Type type;

    @JsonProperty("source")
    private String source = "";

    public enum Type {
        ADD_AGG, ADD_TABLE, REMOVE_AGG, REMOVE_TABLE;

        public boolean isAgg() {
            return this.equals(ADD_AGG) || this.equals(REMOVE_AGG);
        }

        public boolean isTable() {
            return this.equals(ADD_TABLE) || this.equals(REMOVE_AGG);
        }

        public boolean isAdd() {
            return this.equals(ADD_AGG) || this.equals(ADD_TABLE);
        }

        public boolean isRemove() {
            return this.equals(REMOVE_AGG) || this.equals(REMOVE_TABLE);
        }
    }

}
