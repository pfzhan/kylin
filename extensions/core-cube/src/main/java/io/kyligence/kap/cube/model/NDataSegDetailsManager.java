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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeepNames;

/**
 * Package private. Not intended for public use.
 * 
 * Public use goes through NDataflowManager.
 */
class NDataSegDetailsManager implements IKeepNames {
    private static final Serializer<NDataSegDetails> DATA_SEG_CUBOID_INSTANCES_SERIALIZER = new JsonSerializer<>(
            NDataSegDetails.class);

    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetailsManager.class);

    public static NDataSegDetailsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataSegDetailsManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataSegDetailsManager newInstance(KylinConfig config, String project) throws IOException {
        return new NDataSegDetailsManager(config, project);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    private String project;

    private NDataSegDetailsManager(KylinConfig config, String project) throws IOException {
        logger.info("Initializing NDataSegDetailsManager with config " + config);
        this.kylinConfig = config;
        this.project = project;
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }
    
    private NDataSegDetails getForSegment(NDataflow df, int segId) {
        try {
            NDataSegDetails instances = getStore().getResource(getResourcePathForSegment(df.getName(), segId),
                    NDataSegDetails.class, DATA_SEG_CUBOID_INSTANCES_SERIALIZER);
            if (instances != null) {
                instances.setConfig(df.getConfig());
                instances.setProject(project);
            }
            return instances;
        } catch (IOException e) {
            logger.error("Failed to load NDataSegDetails for segment {}", df.getName() + "." + segId, e);
            return null;
        }
    }

    NDataSegDetails getForSegment(NDataSegment segment) {
        return getForSegment(segment.getDataflow(), segment.getId());
    }

    void updateDataflow(NDataflow df, NDataflowUpdate update) throws IOException {

        // figure out all impacted segments
        Set<Integer> allSegIds = new TreeSet<>();
        Map<Integer, List<NDataCuboid>> toUpsert = new TreeMap<>();
        Map<Integer, List<NDataCuboid>> toRemove = new TreeMap<>();
        if (update.getToAddOrUpdateCuboids() != null) {
            for (NDataCuboid c : update.getToAddOrUpdateCuboids()) {
                int segId = c.getSegDetails().getSegmentId();
                allSegIds.add(segId);
                List<NDataCuboid> list = toUpsert.get(segId);
                if (list == null)
                    toUpsert.put(segId, list = new ArrayList<>());
                list.add(c);
            }
        }
        if (update.getToRemoveCuboids() != null) {
            for (NDataCuboid c : update.getToRemoveCuboids()) {
                int segId = c.getSegDetails().getSegmentId();
                allSegIds.add(segId);
                List<NDataCuboid> list = toRemove.get(segId);
                if (list == null)
                    toRemove.put(segId, list = new ArrayList<>());
                list.add(c);
            }
        }
        if (update.getToAddSegs() != null) {
            for (NDataSegment s : update.getToAddSegs()) {
                allSegIds.add(s.getId());
            }
        }

        // upsert for each segment
        for (int segId : allSegIds) {
            NDataSegDetails details = getForSegment(df, segId);
            if (details == null)
                details = NDataSegDetails.newSegDetails(df, segId);

            if (toUpsert.containsKey(segId)) {
                for (NDataCuboid c : toUpsert.get(segId)) {
                    c.setSegDetails(details);
                    details.addCuboid(c);
                }
            }
            if (toRemove.containsKey(segId)) {
                for (NDataCuboid c : toRemove.get(segId)) {
                    details.removeCuboid(c);
                }
            }

            upsertForSegmentQuietly(details);
        }

        if (update.getToRemoveSegs() != null) {
            for (NDataSegment seg : update.getToRemoveSegs()) {
                removeForSegmentQuietly(df, seg.getId());
            }
        }
    }

    private NDataSegDetails upsertForSegmentQuietly(NDataSegDetails details) {
        try {
            return upsertForSegment(details);
        } catch (IOException e) {
            logger.error("Failed to insert/update NDataSegDetails for segment {}",
                    details.getDataflowName() + "." + details.getSegmentId(), e);
            return null;
        }
    }

    NDataSegDetails upsertForSegment(NDataSegDetails details) throws IOException {
        Preconditions.checkNotNull(details, "NDataSegDetails cannot be null.");

        getStore().putResource(details.getResourcePath(), details, DATA_SEG_CUBOID_INSTANCES_SERIALIZER);
        return details;
    }

    private void removeForSegmentQuietly(NDataflow df, int segId) {
        try {
            removeForSegment(df, segId);
        } catch (IOException e) {
            logger.error("Failed to remove NDataSegDetails for segment {}", df.getName() + "." + segId, e);
        }
    }

    void removeForSegment(NDataflow df, int segId) throws IOException {
        getStore().deleteResource(getResourcePathForSegment(df.getName(), segId));
    }

    private String getResourcePathForSegment(String dataflowName, int segId) {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dataflowName + "/" + segId
                + MetadataConstants.FILE_SURFIX;
    }
}
