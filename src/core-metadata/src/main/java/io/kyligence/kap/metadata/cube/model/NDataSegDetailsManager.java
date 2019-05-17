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

package io.kyligence.kap.metadata.cube.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.val;

/**
 * Package private. Not intended for public use.
 * 
 * Public use goes through NDataflowManager.
 */
class NDataSegDetailsManager implements IKeepNames {
    private static final Serializer<NDataSegDetails> DATA_SEG_LAYOUT_INSTANCES_SERIALIZER = new JsonSerializer<>(
            NDataSegDetails.class);

    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetailsManager.class);

    public static NDataSegDetailsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataSegDetailsManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataSegDetailsManager newInstance(KylinConfig config, String project) {
        return new NDataSegDetailsManager(config, project);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    private String project;

    private NDataSegDetailsManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataSegDetailsManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.kylinConfig = config;
        this.project = project;
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private NDataSegDetails getForSegment(NDataflow df, String segId) {
        NDataSegDetails instances = getStore().getResource(getResourcePathForSegment(df.getUuid(), segId),
                DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        if (instances != null) {
            instances.setConfig(df.getConfig());
            instances.setProject(project);
        }
        return instances;
    }

    NDataSegDetails getForSegment(NDataSegment segment) {
        return getForSegment(segment.getDataflow(), segment.getId());
    }

    void updateDataflow(NDataflow df, NDataflowUpdate update) {

        // figure out all impacted segments
        Set<String> allSegIds = new TreeSet<>();
        Map<String, List<NDataLayout>> toUpsert = new TreeMap<>();
        Map<String, List<NDataLayout>> toRemove = new TreeMap<>();
        if (update.getToAddOrUpdateCuboids() != null) {
            Arrays.stream(update.getToAddOrUpdateCuboids()).forEach(c -> {
                val segId = c.getSegDetails().getUuid();
                allSegIds.add(segId);
                List<NDataLayout> list = toUpsert.computeIfAbsent(segId, k -> new ArrayList<>());
                list.add(c);
            });
        }
        if (update.getToRemoveCuboids() != null) {
            Arrays.stream(update.getToRemoveCuboids()).forEach(c -> {
                val segId = c.getSegDetails().getUuid();
                allSegIds.add(segId);
                List<NDataLayout> list = toRemove.computeIfAbsent(segId, k -> new ArrayList<>());
                list.add(c);
            });
        }
        if (update.getToAddSegs() != null) {
            Arrays.stream(update.getToAddSegs()).map(NDataSegment::getId).forEach(allSegIds::add);
        }

        // upsert for each segment
        for (String segId : allSegIds) {
            NDataSegDetails details = getForSegment(df, segId);
            if (details == null)
                details = NDataSegDetails.newSegDetails(df, segId);

            if (toUpsert.containsKey(segId)) {
                for (NDataLayout c : toUpsert.get(segId)) {
                    c.setSegDetails(details);
                    details.addLayout(c);
                }
            }
            if (toRemove.containsKey(segId)) {
                for (NDataLayout c : toRemove.get(segId)) {
                    details.removeLayout(c);
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
        } catch (Exception e) {
            logger.error("Failed to insert/update NDataSegDetails for segment {}",
                    details.getDataflowId() + "." + details.getUuid(), e);
            return null;
        }
    }

    NDataSegDetails upsertForSegment(NDataSegDetails details) {
        Preconditions.checkNotNull(details, "NDataSegDetails cannot be null.");

        getStore().checkAndPutResource(details.getResourcePath(), details, DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        return details;
    }

    private void removeForSegmentQuietly(NDataflow df, String segId) {
        try {
            removeForSegment(df, segId);
        } catch (Exception e) {
            logger.error("Failed to remove NDataSegDetails for segment {}", df + "." + segId, e);
        }
    }

    void removeForSegment(NDataflow df, String segId) {
        getStore().deleteResource(getResourcePathForSegment(df.getUuid(), segId));
    }

    void removeDetails(NDataflow df) {
        val toBeRemoved = getStore().listResourcesRecursively(getResourcePathForDetails(df.getId()));
        if (CollectionUtils.isNotEmpty(toBeRemoved)) {
            toBeRemoved.forEach(path -> getStore().deleteResource(path));
        }
    }

    private String getResourcePathForSegment(String dfId, String segId) {
        return getResourcePathForDetails(dfId) + "/" + segId + MetadataConstants.FILE_SURFIX;
    }

    private String getResourcePathForDetails(String dfId) {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dfId;
    }
}
