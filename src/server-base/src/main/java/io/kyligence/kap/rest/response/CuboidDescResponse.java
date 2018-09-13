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

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.ArrayList;
import java.util.List;

public class CuboidDescResponse {

    @JsonProperty("dimensions_res")
    private List<String> dimensionsRes = new ArrayList<>();
    @JsonProperty("measures_res")
    private List<String> measuresRes = new ArrayList<>();

    @JsonProperty("id")
    private long id;
    @JsonProperty("storage_size")
    private long storageSize;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    @JsonManagedReference
    @JsonProperty("layouts")
    private List<NCuboidLayout> layouts = Lists.newArrayList();

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<String> getDimensionsRes() {
        return dimensionsRes;
    }

    public void setDimensionsRes(List<String> dimensionsRes) {
        this.dimensionsRes = dimensionsRes;
    }

    public List<String> getMeasuresRes() {
        return measuresRes;
    }

    public void setMeasuresRes(List<String> measuresRes) {
        this.measuresRes = measuresRes;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<NCuboidLayout> getLayouts() {
        return layouts;
    }

    public void setLayouts(List<NCuboidLayout> layouts) {
        this.layouts = layouts;
    }

    public NCubePlan getCubePlan() {
        return cubePlan;
    }

    public void setCubePlan(NCubePlan cubePlan) {
        this.cubePlan = cubePlan;
    }

    @JsonBackReference
    private NCubePlan cubePlan;

    public CuboidDescResponse(NCuboidDesc nCuboidDesc) {
        this.setId(nCuboidDesc.getId());
        this.setCubePlan(nCuboidDesc.getCubePlan());
        this.setLayouts(nCuboidDesc.getLayouts());
        ImmutableSet<TblColRef> dimensionSet = nCuboidDesc.getDimensionSet();
        if (!CollectionUtils.isEmpty(dimensionSet)) {

            for (TblColRef dimension : dimensionSet) {
                this.dimensionsRes.add(dimension.getName());
            }
        }

        ImmutableSet<NDataModel.Measure> measureSet = nCuboidDesc.getMeasureSet();
        if (!CollectionUtils.isEmpty(measureSet)) {

            for (NDataModel.Measure measure : measureSet) {
                this.measuresRes.add(measure.getName());
            }
        }
    }

}