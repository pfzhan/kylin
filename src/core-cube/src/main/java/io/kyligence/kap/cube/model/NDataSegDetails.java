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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Holds details of pre-calculated data (like layouts) of a data segment.
 *
 * Could be persisted together with dataflow, but we made it a separated root entity such that
 * - The details of a data segment can be updated concurrently during build.
 * - The update event of data segment is separated from dataflow.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataSegDetails extends RootPersistentEntity implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetailsManager.class);

    public static final String DATAFLOW_DETAILS_RESOURCE_ROOT = "/dataflow_details";

    public static NDataSegDetails newSegDetails(NDataflow df, String segId) {
        NDataSegDetails entity = new NDataSegDetails();
        entity.setConfig(df.getConfig());
        entity.setUuid(segId);
        entity.setDataflowId(df.getUuid());
        entity.setProject(df.getProject());

        List<NDataLayout> cuboids = new ArrayList<>();
        entity.setLayouts(cuboids);
        return entity;
    }

    // ============================================================================

    @JsonProperty("dataflow")
    private String dataflowId;
    @JsonManagedReference
    @JsonProperty("layout_instances")
    private List<NDataLayout> layouts = Lists.newArrayList();

    @JsonIgnore
    private KylinConfigExt config;

    private String project;

    public KylinConfigExt getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public NDataflow getDataflow() {
        return NDataflowManager.getInstance(getConfig(), project).getDataflow(dataflowId);
    }

    public NDataSegment getDataSegment() {
        return getDataflow().getSegment(uuid);
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public String getDataflowId() {
        return dataflowId;
    }

    public void setDataflowId(String dfName) {
        checkIsNotCachedAndShared();
        this.dataflowId = dfName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public long getTotalRowCount() {
        long count = 0L;
        for (NDataLayout cuboid : getLayouts()) {
            count += cuboid.getRows();
        }
        return count;
    }

    public List<NDataLayout> getLayouts() {
        return isCachedAndShared() ? ImmutableList.copyOf(layouts) : layouts;
    }

    public NDataLayout getLayoutById(long layoutId) {
        for (NDataLayout cuboid : getLayouts()) {
            if (cuboid.getLayoutId() == layoutId)
                return cuboid;
        }
        return null;
    }

    public void setLayouts(List<NDataLayout> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    void addLayout(NDataLayout cuboid) {
        checkIsNotCachedAndShared();
        if (layouts.contains(cuboid)) {
            logger.warn("NDataLayout should be immutable, but {} is being updated", cuboid);
            layouts.remove(cuboid); // remove the old cuboid
        }

        layouts.add(cuboid);
    }

    void removeLayout(NDataLayout cuboid) {
        checkIsNotCachedAndShared();
        layouts.remove(cuboid);
    }

    boolean checkLayoutsBeforeMerge(NDataSegDetails another) {

        if (another == this)
            return false;

        List<NDataLayout> currentSortedLayouts = getSortedLayouts(getLayouts());
        List<NDataLayout> anotherSortedLayouts = getSortedLayouts(another.getLayouts());
        int size = currentSortedLayouts.size();
        if (size != anotherSortedLayouts.size())
            return false;

        if (size == 0)
            return true;

        for (int i = 0; i < size; i++) {
            if (currentSortedLayouts.get(i).getLayoutId() != anotherSortedLayouts.get(i).getLayoutId())
                return false;
        }
        return true;
    }

    private static List<NDataLayout> getSortedLayouts(List<NDataLayout> layouts) {
        layouts.sort((o1, o2) -> (int) (o1.getLayoutId() - o2.getLayoutId()));
        return layouts;
    }

    // ============================================================================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dataflowId == null) ? 0 : dataflowId.hashCode());
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
        if (dataflowId == null) {
            return other.dataflowId == null;
        } else return dataflowId.equals(other.dataflowId);
    }

    @Override
    public String toString() {
        return "NDataSegDetails [" + dataflowId + "." + uuid + "]";
    }

    @Override
    public String getResourcePath() {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dataflowId + "/" + uuid
                + MetadataConstants.FILE_SURFIX;
    }

}
