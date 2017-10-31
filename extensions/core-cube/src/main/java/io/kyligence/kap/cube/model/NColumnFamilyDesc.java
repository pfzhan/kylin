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

package io.kyligence.kap.cube.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class NColumnFamilyDesc implements Serializable, IKeep {
    @JsonProperty("name")
    protected String name;

    @JsonProperty("columns")
    protected int[] columns;

    @JsonBackReference
    protected NCuboidLayout layout;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NColumnFamilyDesc that = (NColumnFamilyDesc) o;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        return Arrays.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    public static class DimensionCF extends NColumnFamilyDesc {
    }

    public static class MeasureCF extends NColumnFamilyDesc {
        private Boolean isMemoryHungry = null;

        public boolean isMemoryHungry() {
            if (isMemoryHungry != null)
                return isMemoryHungry;

            synchronized (this) {
                if (isMemoryHungry != null)
                    return isMemoryHungry;

                isMemoryHungry = false;
                Map<Integer, NDataModel.Measure> measuresMap = layout.getCuboidDesc().getOrderedMeasures();
                for (int col : columns) {
                    if (measuresMap.get(col).getFunction().getMeasureType().isMemoryHungry()) {
                        isMemoryHungry = true;
                        break;
                    }
                }
                return isMemoryHungry;
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int[] getColumns() {
        return columns;
    }

    public void setColumns(int[] columns) {
        this.columns = columns;
    }
}
