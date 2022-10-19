/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.delegate;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.request.AddSegmentRequest;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;
import org.apache.kylin.rest.request.MergeSegmentRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;

public interface ModelMetadataContract {

    List<String> getModelNamesByFuzzyName(String fuzzyName, String project);

    String getModelNameById(String modelId, String project);

    Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end);

    String updateSecondStorageModel(String project, String modelId);

    BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request);

    void saveDateFormatIfNotExist(String project, String modelId, String format);

    NDataSegment appendSegment(AddSegmentRequest request);

    NDataSegment refreshSegment(String project, String indexPlanUuid, String segmentId);

    NDataSegment appendPartitions(String project, String dfId, String segId, List<String[]> partitionValues);

    NDataSegment mergeSegments(String project, MergeSegmentRequest mergeSegmentRequest);

    void purgeModelManually(String dataflowId, String project);

    void deleteSegmentById(String model, String project, String[] ids, boolean force);

    void removeIndexesFromSegments(String project, String modelId, List<String> segmentIds,
                                   List<Long> indexIds);

    void updateIndex(String project, long epochId, String modelId, Set<Long> toBeDeletedLayoutIds, boolean deleteAuto,
                     boolean deleteManual);

    void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest);

    void updateDataflow(String project, String dfId, String segmentId, long maxBucketIt);

    void updateIndexPlan(String project, String uuid, IndexPlan indexplan, String action);

    void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status);

    void updateRecommendationsCount(String project, String modelId, int size);
}
