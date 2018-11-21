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

package io.kyligence.kap.engine.spark.builder;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NDataModel;

public class NSnapshotBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(NSnapshotBuilder.class);
    private SparkSession ss;
    private NDataSegment seg;

    public NSnapshotBuilder(NDataSegment seg, SparkSession ss) {
        this.seg = seg;
        this.ss = ss;
    }

    public NDataSegment buildSnapshot() throws IOException {
        logger.info("building snapshots for seg {}", seg);
        NDataModel model = (NDataModel) seg.getDataflow().getModel();

        Map<String, String> newSnapMap = Maps.newHashMap();
        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            TableDesc tableDesc = lookupDesc.getTableRef().getTableDesc();
            boolean isLookupTable = model.isLookupTable(lookupDesc.getTableRef());

            if (isLookupTable && seg.getSnapshots().get(tableDesc.getIdentity()) == null) {

                Dataset<Row> sourceData = SourceFactory
                        .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                        .getSourceData(tableDesc, ss, Maps.newHashMap());
                String tableName = tableDesc.getProject() + ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/"
                        + tableDesc.getName() + "/" + UUID.randomUUID();
                String resourcePath = KapConfig.wrap(seg.getConfig()).getReadHdfsWorkingDirectory() + "/" + tableName;
                sourceData.write().parquet(resourcePath);
                newSnapMap.put(tableDesc.getIdentity(), resourcePath);
            }
        }

        final NDataflow dataflow = seg.getDataflow();

        // make a copy of the changing segment, avoid changing the cached object
        NDataflow dfCopy = seg.getDataflow().copy();
        NDataSegment segCopy = dfCopy.getSegment(seg.getId());

        try {
            NDataflowUpdate update = new NDataflowUpdate(dataflow.getName());
            segCopy.getSnapshots().putAll(newSnapMap);
            update.setToUpdateSegs(segCopy);
            NDataflow updatedDataflow = NDataflowManager.getInstance(seg.getConfig(), seg.getProject())
                    .updateDataflow(update);
            return updatedDataflow.getSegment(seg.getId());

        } catch (IOException e) {
            throw new RuntimeException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

}
