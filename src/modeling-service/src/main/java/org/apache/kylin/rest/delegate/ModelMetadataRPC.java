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
import org.apache.kylin.rest.aspect.WaitForSyncAfterRPC;
import org.apache.kylin.rest.request.AddSegmentRequest;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;
import org.apache.kylin.rest.request.MergeSegmentRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelSuggestionRequest;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.OpenRecApproveResponse;
import org.apache.kylin.rest.response.OptRecResponse;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/models/feign")
public interface ModelMetadataRPC extends ModelMetadataContract {

    @PostMapping(value = "/update_index")
    @WaitForSyncAfterRPC
    void updateIndex(@RequestParam("project") String project, @RequestParam("epochId") long epochId,
            @RequestParam("modelId") String modelId, @RequestBody Set<Long> toBeDeletedLayoutIds,
            @RequestParam("deleteAuto") boolean deleteAuto, @RequestParam("deleteManual") boolean deleteManual);

    @PostMapping(value = "/update_dataflow")
    @WaitForSyncAfterRPC
    void updateDataflow(@RequestBody DataFlowUpdateRequest dataFlowUpdateRequest);

    @PostMapping(value = "/update_dataflow_maxBucketId")
    @WaitForSyncAfterRPC
    void updateDataflow(@RequestParam("project") String project, @RequestParam("dfId") String dfId,
            @RequestParam("segmentId") String segmentId, @RequestParam("maxBucketId") long maxBucketIt);

    @PostMapping(value = "/update_index_plan")
    @WaitForSyncAfterRPC
    void updateIndexPlan(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
            @RequestBody IndexPlan indexplan, @RequestParam("action") String action);

    @PostMapping(value = "/update_dataflow_status")
    @WaitForSyncAfterRPC
    void updateDataflowStatus(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
            @RequestParam("status") RealizationStatusEnum status);

    @PostMapping(value = "/get_model_id_by_fuzzy_name")
    List<String> getModelNamesByFuzzyName(@RequestParam("fuzzyName") String fuzzyName,
            @RequestParam("project") String project);

    @PostMapping(value = "/get_model_name_by_id")
    String getModelNameById(@RequestParam("modelId") String modelId, @RequestParam("project") String project);

    @PostMapping(value = "/get_segment_by_range")
    Segments<NDataSegment> getSegmentsByRange(@RequestParam("modelId") String modelId,
            @RequestParam("project") String project, @RequestParam("start") String start,
            @RequestParam("end") String end);

    @PostMapping(value = "/update_second_storage_model")
    @WaitForSyncAfterRPC
    String updateSecondStorageModel(@RequestParam("project") String project, @RequestParam("modelId") String modelId);

    @PostMapping(value = "/update_data_model_semantic")
    @WaitForSyncAfterRPC
    BuildBaseIndexResponse updateDataModelSemantic(@RequestParam("project") String project,
            @RequestBody ModelRequest request);

    @PostMapping(value = "/save_data_format_if_not_exist")
    @WaitForSyncAfterRPC
    void saveDateFormatIfNotExist(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("format") String format);

    @PostMapping(value = "/append_segment")
    @WaitForSyncAfterRPC
    NDataSegment appendSegment(@RequestBody AddSegmentRequest request);

    @PostMapping(value = "/refresh_segment")
    @WaitForSyncAfterRPC
    NDataSegment refreshSegment(@RequestParam("project") String project,
            @RequestParam("indexPlanUuid") String indexPlanUuid, @RequestParam("segmentId") String segmentId);

    @PostMapping(value = "/append_partitions")
    @WaitForSyncAfterRPC
    NDataSegment appendPartitions(@RequestParam("project") String project, @RequestParam("dfIF") String dfId,
            @RequestParam("segId") String segId, @RequestBody List<String[]> partitionValues);

    @PostMapping(value = "/merge_segments")
    @WaitForSyncAfterRPC
    NDataSegment mergeSegments(@RequestParam("project") String project,
            @RequestBody MergeSegmentRequest mergeSegmentRequest);

    @PostMapping(value = "/purge_model_manually")
    void purgeModelManually(@RequestParam("dataflowId") String dataflowId, @RequestParam("project") String project);

    @PostMapping(value = "/delete_segment_by_id")
    @WaitForSyncAfterRPC
    void deleteSegmentById(@RequestParam("model") String model, @RequestParam("project") String project,
            @RequestBody String[] ids, @RequestParam("force") boolean force);

    @PostMapping(value = "/remove_indexes_from_segments")
    @WaitForSyncAfterRPC
    void removeIndexesFromSegments(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("segmentIds") List<String> segmentIds, @RequestParam("indexIds") List<Long> indexIds);

    @PostMapping(value = "/batch_save_models")
    @WaitForSyncAfterRPC
    void batchCreateModel(@RequestBody ModelSuggestionRequest request);

    @PostMapping(value = "/update_recommendations_count")
    @WaitForSyncAfterRPC
    void updateRecommendationsCount(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("size") int size);

    @PostMapping(value = "/approve")
    @WaitForSyncAfterRPC
    OptRecResponse approve(@RequestParam("project") String project, @RequestBody OptRecRequest request);

    @PostMapping(value = "/approve_all_rec_items")
    @WaitForSyncAfterRPC
    OpenRecApproveResponse.RecToIndexResponse approveAllRecItems(@RequestParam("project") String project,
            @RequestParam("modelId") String modelId, @RequestParam("modelAlias") String modelAlias,
            @RequestParam("recActionType") String recActionType);

    @PostMapping(value = "/save_new_models_and_indexes")
    @WaitForSyncAfterRPC
    void saveNewModelsAndIndexes(@RequestParam("project") String project, @RequestBody List<ModelRequest> newModels);

    @PostMapping(value = "/save_rec_result")
    @WaitForSyncAfterRPC
    void saveRecResult(@RequestBody SuggestionResponse newModels, @RequestParam("project") String project);

    @PostMapping(value = "/update_models")
    @WaitForSyncAfterRPC
    void updateModels(@RequestBody List<SuggestionResponse.ModelRecResponse> reusedModels,
            @RequestParam("project") String project);
}
