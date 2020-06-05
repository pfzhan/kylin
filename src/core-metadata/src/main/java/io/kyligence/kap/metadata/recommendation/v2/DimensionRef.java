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

package io.kyligence.kap.metadata.recommendation.v2;

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DimensionRef extends RecommendationRef {
    private ColumnRef columnRef;

    public DimensionRef(int id) {
        this.id = id;
    }

    public DimensionRef(ColumnRef columnRef, int id, boolean existed) {
        this.columnRef = columnRef;
        this.id = id;
        this.existed = existed;
    }

    @Override
    public List<RecommendationRef> getDependencies() {
        return validate(Lists.newArrayList(columnRef));
    }

    @Override
    public String getContent() {
        DimensionContent content = new DimensionContent(columnRef.getName(), columnRef.getDataType());
        return JsonUtil.writeValueAsStringQuietly(content);
    }

    @Override
    public String getName() {
        return columnRef.getName().replace(".", "_");
    }

    public int getColId() {
        return columnRef.getId();
    }

    @Setter
    @Getter
    @AllArgsConstructor
    public static class DimensionContent {
        private String column;
        @JsonProperty("data_type")
        private String dataType;
    }
}
