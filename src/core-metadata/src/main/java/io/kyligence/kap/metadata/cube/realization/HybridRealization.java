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

package io.kyligence.kap.metadata.cube.realization;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;

public class HybridRealization implements IRealization {

    public static final String REALIZATION_TYPE = "NHYBRID";

    private final static Logger logger = LoggerFactory.getLogger(HybridRealization.class);

    @Getter
    private String uuid;
    private int cost = 50;
    private volatile List<IRealization> realizations = new ArrayList<>();
    private volatile IRealization batchRealization;
    private volatile IRealization streamingRealization;
    private String project;

    private List<TblColRef> allDimensions = null;
    private Set<TblColRef> allColumns = null;
    private Set<ColumnDesc> allColumnDescs = null;
    private List<MeasureDesc> allMeasures = null;
    private long dateRangeStart;
    private long dateRangeEnd;
    private boolean isReady = false;
    private KylinConfigExt config;

    public HybridRealization(IRealization batchRealization, IRealization streamingRealization, String project) {
        if (batchRealization == null && streamingRealization == null) {
            return;
        }
        this.batchRealization = batchRealization;
        this.streamingRealization = streamingRealization;
        this.realizations.add(batchRealization);
        this.realizations.add(streamingRealization);
        this.project = project;

        LinkedHashSet<TblColRef> columns = new LinkedHashSet<>();
        LinkedHashSet<TblColRef> dimensions = new LinkedHashSet<>();
        dateRangeStart = 0;
        dateRangeEnd = Long.MAX_VALUE;
        for (IRealization realization : realizations) {
            columns.addAll(realization.getAllColumns());
            dimensions.addAll(realization.getAllDimensions());

            if (realization.isReady())
                isReady = true;

            if (dateRangeStart == 0 || realization.getDateRangeStart() < dateRangeStart)
                dateRangeStart = realization.getDateRangeStart();

            if (dateRangeStart == Long.MAX_VALUE || realization.getDateRangeEnd() > dateRangeEnd)
                dateRangeEnd = realization.getDateRangeEnd();
        }

        allDimensions = Lists.newArrayList(dimensions);
        allColumns = columns;
        allColumnDescs = asColumnDescs(allColumns);
        allMeasures = Lists.newArrayList(streamingRealization.getMeasures());
        uuid = streamingRealization.getUuid();

        Collections.sort(realizations, (realization1, realization2) -> {
            long dateRangeStart1 = realization1.getDateRangeStart();
            long dateRangeStart2 = realization2.getDateRangeStart();
            long comp = dateRangeStart1 - dateRangeStart2;
            if (comp != 0) {
                return comp > 0 ? 1 : -1;
            }

            dateRangeStart1 = realization1.getDateRangeEnd();
            dateRangeStart2 = realization2.getDateRangeEnd();
            comp = dateRangeStart1 - dateRangeStart2;
            if (comp != 0) {
                return comp > 0 ? 1 : -1;
            }

            return 0;
        });
    }

    private Set<ColumnDesc> asColumnDescs(Set<TblColRef> columns) {
        LinkedHashSet<ColumnDesc> result = new LinkedHashSet<>();
        for (TblColRef col : columns) {
            result.add(col.getColumnDesc());
        }
        return result;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments) {
        return new CapabilityResult();
    }

    public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments,
            List<NDataSegment> prunedStreamingSegments) {
        CapabilityResult result = new CapabilityResult();
        result.cost = Integer.MAX_VALUE;

        if (streamingRealization != null) {
            CapabilityResult streamingChild = streamingRealization.isCapable(digest, prunedStreamingSegments);
            result.setSelectedStreamingCandidate(streamingChild.getSelectedStreamingCandidate());
        }

        for (IRealization realization : getRealizations()) {
            CapabilityResult child;
            if (realization.isStreaming()) {
                child = realization.isCapable(digest, prunedStreamingSegments);
                result.setSelectedStreamingCandidate(child.getSelectedStreamingCandidate());
                if (child.capable) {
                    result.cost = Math.min(result.cost, (int)child.getSelectedStreamingCandidate().getCost());
                }
            } else {
                child = realization.isCapable(digest, prunedSegments);
                result.setSelectedCandidate(child.getSelectedCandidate());
                if (child.capable) {
                    result.cost = Math.min(result.cost, (int)child.getSelectedCandidate().getCost());
                }
            }
            if (child.capable) {
                result.capable = true;
                result.influences.addAll(child.influences);
            } else {
                result.incapableCause = child.incapableCause;
            }
        }

        if (result.cost > 0)
            result.cost--; // let hybrid win its children

        return result;
    }

    @Override
    public int getCost() {
        int c = Integer.MAX_VALUE;
        for (IRealization realization : getRealizations()) {
            c = Math.min(realization.getCost(), c);
        }
        // let hybrid cost win its children
        cost = --c;
        return cost;
    }

    public List<IRealization> getRealizations() {
        return realizations;
    }

    public IRealization getBatchRealization() {
        return batchRealization;
    }

    public IRealization getStreamingRealization() {
        return streamingRealization;
    }

    @Override
    public String getType() {
        return REALIZATION_TYPE;
    }

    @Override
    public KylinConfigExt getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    @Override
    public NDataModel getModel() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataModelDesc(uuid);
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return allColumns;
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return allColumnDescs;
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return allMeasures;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return allDimensions;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + getModel().getAlias() + "]";
    }

    @Override
    public long getDateRangeStart() {
        return dateRangeStart;
    }

    @Override
    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    @Override
    public boolean supportsLimitPushDown() {
        return false;
    }

    @Override
    public boolean hasPrecalculatedFields() {
        return true;
    }

    @Override
    public int getStorageType() {
        return IStorageAware.ID_NDATA_STORAGE;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setProject(String project) {
        this.project = project;
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
