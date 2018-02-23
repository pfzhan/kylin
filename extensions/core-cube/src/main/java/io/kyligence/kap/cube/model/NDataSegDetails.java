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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Holds details of pre-calculated data (like cuboids) of a data segment.
 * 
 * Could be persisted together with dataflow, but we made it a separated root entity such that
 * - The details of a data segment can be updated concurrently during build.
 * - The update event of data segment is separated from dataflow.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataSegDetails extends RootPersistentEntity {

    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetailsManager.class);

    public static final String DATAFLOW_DETAILS_RESOURCE_ROOT = "/dataflow_details";

    public static NDataSegDetails newSegDetails(NDataflow df, int segId) {
        NDataSegDetails entity = new NDataSegDetails();
        entity.setConfig(df.getConfig());
        entity.setUuid(UUID.randomUUID().toString());
        entity.setSegmentId(segId);
        entity.setDataflowName(df.getName());

        List<NDataCuboid> cuboids = new ArrayList<>();
        entity.setCuboids(cuboids);
        return entity;
    }

    // ============================================================================

    @JsonProperty("dataflow")
    private String dataflowName;
    @JsonProperty("segment_id")
    private int segmentId;
    @JsonManagedReference
    @JsonProperty("cuboid_instances")
    private List<NDataCuboid> cuboids = Lists.newArrayList();

    @JsonIgnore
    private KylinConfigExt config;

    public String getResourcePath() {
        return NDataSegDetailsManager.getResourcePathForSegment(dataflowName, segmentId);
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public NDataflow getDataflow() {
        return NDataflowManager.getInstance(getConfig()).getDataflow(dataflowName);
    }

    public NDataSegment getDataSegment() {
        return getDataflow().getSegment(segmentId);
    }

    public Map<Long, Long> getCuboidRowsMap() {
        Map<Long, Long> cuboidRows = Maps.newHashMap();
        for (NDataCuboid cuboid : cuboids) {
            cuboidRows.put(cuboid.getCuboidLayoutId(), cuboid.getRows());
        }
        return cuboidRows;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public String getDataflowName() {
        return dataflowName;
    }

    public void setDataflowName(String dfName) {
        checkIsNotCachedAndShared();
        this.dataflowName = dfName;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        checkIsNotCachedAndShared();
        this.segmentId = segmentId;
    }

    public long getSizeKB() {
        long sizeKB = 0L;
        for (NDataCuboid cuboid : getCuboids()) {
            sizeKB += cuboid.getSizeKB();
        }
        return sizeKB;
    }

    public List<NDataCuboid> getCuboids() {
        return isCachedAndShared() ? ImmutableList.copyOf(cuboids) : cuboids;
    }

    public NDataCuboid getCuboidById(long layoutId) {
        for (NDataCuboid cuboid : getCuboids()) {
            if (cuboid.getCuboidLayoutId() == layoutId)
                return cuboid;
        }
        return null;
    }

    public void setCuboids(List<NDataCuboid> cuboids) {
        checkIsNotCachedAndShared();
        this.cuboids = cuboids;
    }

    public void addCuboid(NDataCuboid cuboid) {
        checkIsNotCachedAndShared();
        if (cuboids.contains(cuboid)) {
            logger.warn("NDataCuboid should be immutable, but {} is being updated", cuboid);
            cuboids.remove(cuboid); // remove the old cuboid
        }

        cuboids.add(cuboid);
    }

    public void removeCuboid(NDataCuboid cuboid) {
        checkIsNotCachedAndShared();
        cuboids.remove(cuboid);
    }

    public List<NDataCuboid> getCuboidByStatus(SegmentStatusEnum status) {
        List<NDataCuboid> expectedList = Lists.newArrayList();
        for (NDataCuboid cuboid : getCuboids()) {
            if (cuboid.getStatus() == status)
                expectedList.add(cuboid);
        }
        return expectedList;
    }

    public boolean checkCuboidsBeforeMerge(NDataSegDetails another) {

        if (another == this)
            return false;

        List<NDataCuboid> currentSortedCuboids = getSortedCuboids(getCuboids());
        List<NDataCuboid> anotherSortedCuboids = another.getSortedCuboids(another.getCuboids());
        int size = currentSortedCuboids.size();
        if (size != anotherSortedCuboids.size())
            return false;

        if (size == 0)
            return true;

        for (int i = 0; i < size; i++) {
            if (currentSortedCuboids.get(i).getCuboidLayoutId() != anotherSortedCuboids.get(i).getCuboidLayoutId())
                return false;

            if (currentSortedCuboids.get(i).getStatus() != anotherSortedCuboids.get(i).getStatus())
                return false;
        }

        if (getCuboidByStatus(SegmentStatusEnum.READY).size() < 1)
            return false;

        return true;
    }

    public static List<NDataCuboid> getSortedCuboids(List<NDataCuboid> cuboids) {
        Collections.sort(cuboids, new Comparator<NDataCuboid>() {
            @Override
            public int compare(NDataCuboid o1, NDataCuboid o2) {
                return (int) (o1.getCuboidLayoutId() - o2.getCuboidLayoutId());
            }
        });
        return cuboids;
    }

    // ============================================================================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dataflowName == null) ? 0 : dataflowName.hashCode());
        result = prime * result + segmentId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        NDataSegDetails other = (NDataSegDetails) obj;
        if (dataflowName == null) {
            if (other.dataflowName != null)
                return false;
        } else if (!dataflowName.equals(other.dataflowName))
            return false;
        if (segmentId != other.segmentId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "NDataSegDetails [" + dataflowName + "." + segmentId + "]";
    }

}
