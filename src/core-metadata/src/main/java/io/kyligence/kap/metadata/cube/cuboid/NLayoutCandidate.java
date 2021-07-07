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
package io.kyligence.kap.metadata.cube.cuboid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Getter;
import lombok.Setter;

public class NLayoutCandidate implements IRealizationCandidate {
    private @Nonnull LayoutEntity layoutEntity;
    @Setter
    private double cost;

    @Setter
    @Getter
    private CapabilityResult capabilityResult;

    public static final NLayoutCandidate EMPTY = new NLayoutCandidate(new LayoutEntity(), Double.MAX_VALUE, new CapabilityResult());

    // derived
    private @Nonnull Map<TblColRef, DeriveInfo> derivedToHostMap = Maps.newHashMap();

    public NLayoutCandidate(@Nonnull LayoutEntity layoutEntity) {
        this.layoutEntity = layoutEntity;
    }

    public NLayoutCandidate(@Nonnull LayoutEntity layoutEntity, double cost, CapabilityResult result) {
        this.layoutEntity = layoutEntity;
        this.cost = cost;
        this.capabilityResult = result;
    }

    public boolean isEmptyCandidate() {
        return this.getLayoutEntity().getIndex() == null;
    }

    @Nonnull
    public LayoutEntity getLayoutEntity() {
        return layoutEntity;
    }

    public void setLayoutEntity(@Nonnull LayoutEntity cuboidLayout) {
        this.layoutEntity = cuboidLayout;
    }

    @Nonnull
    public Map<TblColRef, DeriveInfo> getDerivedToHostMap() {
        return derivedToHostMap;
    }

    public void setDerivedToHostMap(@Nonnull Map<TblColRef, DeriveInfo> derivedToHostMap) {
        this.derivedToHostMap = derivedToHostMap;
    }

    public Map<Array<TblColRef>, List<DeriveInfo>> makeHostToDerivedMap() {
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

        for (Map.Entry<TblColRef, DeriveInfo> entry : derivedToHostMap.entrySet()) {

            TblColRef derCol = entry.getKey();
            TblColRef[] hostCols = entry.getValue().columns;
            DeriveInfo.DeriveType type = entry.getValue().type;
            JoinDesc join = entry.getValue().join;

            Array<TblColRef> hostColArray = new Array<>(hostCols);
            List<DeriveInfo> infoList = hostToDerivedMap.get(hostColArray);
            if (infoList == null) {
                infoList = new ArrayList<DeriveInfo>();
                hostToDerivedMap.put(hostColArray, infoList);
            }

            // Merged duplicated derived column
            boolean merged = false;
            for (DeriveInfo existing : infoList) {
                if (existing.type == type && existing.join.getPKSide().equals(join.getPKSide())) {
                    if (ArrayUtils.contains(existing.columns, derCol)) {
                        merged = true;
                        break;
                    }
                    if (type == DeriveInfo.DeriveType.LOOKUP || type == DeriveInfo.DeriveType.LOOKUP_NON_EQUI) {
                        existing.columns = (TblColRef[]) ArrayUtils.add(existing.columns, derCol);
                        merged = true;
                        break;
                    }
                }
            }
            if (!merged)
                infoList.add(new DeriveInfo(type, join, new TblColRef[] { derCol }, false));
        }

        return hostToDerivedMap;
    }

    @Override
    public double getCost() {
        return this.cost;
    }

    @Override
    public String toString() {
        return "NLayoutCandidate{" + "cuboidLayout=" + layoutEntity + ", indexEntity=" + layoutEntity.getIndex()
                + ", cost=" + cost + '}';
    }
}