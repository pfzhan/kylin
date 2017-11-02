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

import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.CubeInstanceResponse;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.rest.service.KapCubeService;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KapCubeResponse extends CubeInstanceResponse {

    public static KapCubeResponse create(CubeInstance cube, KapCubeService kapCubeService) {
        Preconditions.checkState(!cube.getDescriptor().isDraft());

        KapCubeResponse ret = new KapCubeResponse(cube, kapCubeService.getProjectOfCube(cube));

        // MP master should reflect a summary of all MP cubes segments
        Segments<CubeSegment> segments = cube.getSegments();
        MPCubeManager mgr = MPCubeManager.getInstance(cube.getConfig());
        if (mgr.isMPMaster(cube)) {
            segments = collectAllMPCubeSegments(mgr.listMPCubes(cube));
            // summarize sizes and counts
            ret.setSegments(segments);
            ret.initSizeKB();
            ret.initInputRecordCount();
            ret.initInputRecordSizeMB();
            ret.setSegments(new Segments());
        }

        ret.totalStorageSizeKB = kapCubeService.computeSegmentsStorage(cube, segments);

        // MPMaster expose one fake segment to indicate the build time
        if (mgr.isMPMaster(cube)) {
            Segments<CubeSegment> cubeSegments = findLatestSegment(cube, segments);

            ret.setSegments(cubeSegments);
        }

        return ret;
    }

    private static Segments<CubeSegment> collectAllMPCubeSegments(List<CubeInstance> mpCubes) {
        Segments<CubeSegment> result = new Segments<>();
        for (CubeInstance c : mpCubes) {
            result.addAll(c.getSegments());
        }
        return result;
    }

    private static Segments<CubeSegment> findLatestSegment(CubeInstance cube, List<CubeSegment> segments) {
        if (segments.size() == 0)
            return new Segments<CubeSegment>();

        CubeSegment latest = null;
        for (CubeSegment s : segments) {
            if (latest == null || latest.getLastBuildTime() < s.getLastBuildTime())
                latest = s;
        }

        Segments<CubeSegment> result = new Segments<>();
        CubeSegment seg = new CubeSegment();
        if (latest != null) {
            seg.setLastBuildTime(latest.getLastBuildTime());
        }
        result.add(seg);

        seg.setCubeInstance(cube);
        SegmentRange.TSRange tsRange = new SegmentRange.TSRange(cube.getDescriptor().getPartitionDateStart(),
                cube.getDescriptor().getPartitionDateStart());
        seg.setTSRange(tsRange);
        return result;
    }

    public static KapCubeResponse create(Draft d) {
        CubeDesc desc = (CubeDesc) d.getEntity();
        Preconditions.checkState(desc.isDraft());

        KapCubeResponse r = new KapCubeResponse(null, d.getProject());
        r.setName(desc.getName());
        r.setDescName(desc.getName());
        r.setStatus(RealizationStatusEnum.DISABLED);
        r.setModel(desc.getModelName());
        r.setLastModified(d.getLastModified());
        return r;
    }

    // ============================================================================

    @JsonProperty("is_draft")
    private boolean isDraft;
    @JsonProperty("multilevel_partition_cols")
    private String[] mpCols;
    @JsonProperty("total_storage_size_kb")
    private long totalStorageSizeKB;

    private KapCubeResponse(CubeInstance cube, String prj) {
        super(cube, prj);

        this.isDraft = (cube == null);

        initMPCols(cube);
    }

    private void initMPCols(CubeInstance cube) {
        if (cube == null || cube.getModel() == null) // tolerate bad (missing) model
            return;

        KapModel model = (KapModel) cube.getModel();
        TblColRef[] cols = model.getMutiLevelPartitionCols();
        if (cols == null)
            cols = new TblColRef[0];

        mpCols = new String[cols.length];
        for (int i = 0; i < mpCols.length; i++) {
            mpCols[i] = cols[i].getIdentity();
        }
    }

}
