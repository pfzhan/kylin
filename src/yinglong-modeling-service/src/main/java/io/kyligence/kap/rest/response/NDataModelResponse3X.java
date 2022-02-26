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
import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;

import java.util.List;
import java.util.Set;

@Getter
@Setter
public class NDataModelResponse3X extends NDataModel {
    @JsonProperty("status")
    private String status;

    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("source")
    private long source;

    @JsonProperty("expansion_rate")
    private String expansionrate;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("model_broken")
    private boolean modelBroken;

    @JsonProperty("root_fact_table_deleted")
    private boolean rootFactTableDeleted = false;

    @JsonProperty("segments")
    private List<NDataSegmentResponse> segments;

    @JsonProperty("recommendations_count")
    private int recommendationsCount;

    @JsonProperty("available_indexes_count")
    private long availableIndexesCount;

    @JsonProperty("empty_indexes_count")
    private long emptyIndexesCount;

    @JsonProperty("segment_holes")
    private List<SegmentRange> segmentHoles;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    private long lastModify;

    @JsonProperty("simplified_dimensions")
    private List<NDataModel.NamedColumn> namedColumns;

    @JsonProperty("all_measures")
    private List<NDataModel.Measure> measures;

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures;

    @JsonProperty("name")
    private String name;

    @JsonProperty("lookups")
    private List<JoinTableDesc> joinTables;

    @JsonProperty("is_streaming")
    private boolean streaming;

    @JsonProperty("size_kb")
    private long sizeKB;

    @JsonProperty("input_records_count")
    private long inputRecordCnt;

    @JsonProperty("input_records_size")
    private long inputRecordSizeBytes;

    @JsonProperty("project")
    private String projectName;

    @JsonProperty("unauthorized_tables")
    private Set<String> unauthorizedTables = Sets.newHashSet();

    @JsonProperty("unauthorized_columns")
    private Set<String> unauthorizedColumns = Sets.newHashSet();

    @JsonProperty("visible")
    public boolean isVisible() {
        return unauthorizedTables.isEmpty() && unauthorizedColumns.isEmpty();
    }

    public enum ModelStatus3XEnum {
        READY, DISABLED, WARNING, DESCBROKEN;

        public static ModelStatus3XEnum convert(ModelStatusToDisplayEnum modelStatusToDisplayEnum) {
            if (null == modelStatusToDisplayEnum) {
                return null;
            }

            switch (modelStatusToDisplayEnum) {
            case ONLINE:
                return READY;
            case OFFLINE:
                return DISABLED;
            case WARNING:
                return WARNING;
            case BROKEN:
                return DESCBROKEN;
            default:
                break;
            }
            return null;
        }
    }

    public static NDataModelResponse3X convert(NDataModelResponse nDataModelResponse) throws Exception {
        NDataModelResponse3X nDataModelResponse3X = JsonUtil.readValue(JsonUtil.writeValueAsString(nDataModelResponse),
                NDataModelResponse3X.class);
        ModelStatus3XEnum newStatus = ModelStatus3XEnum.convert(nDataModelResponse.getStatus());
        nDataModelResponse3X.setStatus(null == newStatus ? null : newStatus.name());
        nDataModelResponse3X.setMvcc(nDataModelResponse.getMvcc());

        return nDataModelResponse3X;
    }
}
