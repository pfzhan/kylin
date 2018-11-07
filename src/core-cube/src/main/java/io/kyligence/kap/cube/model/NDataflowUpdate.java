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

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

public class NDataflowUpdate {

    private final String dataflowName;
    
    private NDataSegment[] toAddSegs = null;
    private NDataSegment[] toRemoveSegs = null;
    private NDataSegment[] toUpdateSegs = null;
    
    private NDataCuboid[] toAddOrUpdateCuboids = null;
    private NDataCuboid[] toRemoveCuboids = null;

    private RealizationStatusEnum status;
    private String description;
    private String owner;
    private int cost = -1;

    public NDataflowUpdate(String dfName) {
        this.dataflowName = dfName;
    }
    
    public String getDataflowName() {
        return dataflowName;
    }

    public NDataSegment[] getToAddSegs() {
        return toAddSegs;
    }

    public NDataflowUpdate setToAddSegs(NDataSegment... toAddSegs) {
        for (NDataSegment seg : toAddSegs)
            seg.checkIsNotCachedAndShared();
        
        this.toAddSegs = toAddSegs;
        return this;
    }

    public NDataSegment[] getToRemoveSegs() {
        return toRemoveSegs;
    }

    public NDataflowUpdate setToRemoveSegs(NDataSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataflowUpdate setToRemoveSegsWithArray(NDataSegment[] toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataSegment[] getToUpdateSegs() {
        return toUpdateSegs;
    }

    public NDataflowUpdate setToUpdateSegs(NDataSegment... toUpdateSegs) {
        for (NDataSegment seg : toUpdateSegs)
            seg.checkIsNotCachedAndShared();
        
        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public NDataCuboid[] getToAddOrUpdateCuboids() {
        return toAddOrUpdateCuboids;
    }

    public void setToAddOrUpdateCuboids(NDataCuboid... toAddCuboids) {
        for (NDataCuboid cuboid : toAddCuboids)
            cuboid.checkIsNotCachedAndShared();
        
        this.toAddOrUpdateCuboids = toAddCuboids;
    }

    public NDataCuboid[] getToRemoveCuboids() {
        return toRemoveCuboids;
    }

    public void setToRemoveCuboids(NDataCuboid... toRemoveCuboids) {
        this.toRemoveCuboids = toRemoveCuboids;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public NDataflowUpdate setStatus(RealizationStatusEnum status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOwner() {
        return owner;
    }

    public NDataflowUpdate setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public int getCost() {
        return cost;
    }

    public NDataflowUpdate setCost(int cost) {
        this.cost = cost;
        return this;
    }
}
