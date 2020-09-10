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

import java.io.Serializable;
import java.util.List;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
public class LayoutRecDetailResponse implements Serializable {
    @JsonProperty("sqls")
    private List<String> sqlList = Lists.newArrayList();
    @JsonProperty("index_id")
    private long indexId;
    @JsonProperty("dimensions")
    private List<RecDimension> dimensions = Lists.newArrayList();
    @JsonProperty("measures")
    private List<RecMeasure> measures = Lists.newArrayList();
    @JsonProperty("computed_columns")
    private List<RecComputedColumn> computedColumns = Lists.newArrayList();

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecDimension implements Serializable {
        private NDataModel.NamedColumn dimension;
        private String dataType;
        private boolean isNew;

        public RecDimension(NDataModel.NamedColumn dimension, boolean isNew, String dataType) {
            this.dataType = dataType;
            this.dimension = dimension;
            this.isNew = isNew;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecMeasure implements Serializable {
        private NDataModel.Measure measure;
        private boolean isNew;

        public RecMeasure(NDataModel.Measure measure, boolean isNew) {
            this.measure = measure;
            this.isNew = isNew;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecComputedColumn implements Serializable {
        private ComputedColumnDesc cc;
        private boolean isNew;

        public RecComputedColumn(ComputedColumnDesc cc, boolean isNew) {
            this.cc = cc;
            this.isNew = isNew;
        }
    }

}
