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

@Getter
@Setter
public class PreReloadTableResponse {

    @JsonProperty("add_column_count")
    private long addColumnCount;

    @JsonProperty("remove_column_count")
    private long removeColumnCount;

    @JsonProperty("data_type_change_column_count")
    private long dataTypeChangeColumnCount;

    @JsonProperty("broken_model_count")
    private long brokenModelCount;

    @JsonProperty("remove_measures_count")
    private long removeMeasureCount;

    @JsonProperty("remove_dimensions_count")
    private long removeDimCount;

    @JsonProperty("remove_layouts_count")
    private long removeLayoutsCount;

    @JsonProperty("add_layouts_count")
    private long addLayoutsCount;

    @JsonProperty("refresh_layouts_count")
    private long refreshLayoutsCount;

    @JsonProperty("snapshot_deleted")
    private boolean snapshotDeleted = false;

    @JsonProperty("update_base_index_count")
    private int updateBaseIndexCount;

    public PreReloadTableResponse() {
    }

    public PreReloadTableResponse(PreReloadTableResponse otherResponse) {
        this.addColumnCount = otherResponse.addColumnCount;
        this.removeColumnCount = otherResponse.removeColumnCount;
        this.dataTypeChangeColumnCount = otherResponse.dataTypeChangeColumnCount;
        this.brokenModelCount = otherResponse.brokenModelCount;
        this.removeMeasureCount = otherResponse.removeMeasureCount;
        this.removeDimCount = otherResponse.removeDimCount;
        this.removeLayoutsCount = otherResponse.removeLayoutsCount;
        this.addLayoutsCount = otherResponse.addLayoutsCount;
        this.refreshLayoutsCount = otherResponse.refreshLayoutsCount;
        this.snapshotDeleted = otherResponse.snapshotDeleted;
    }
}
