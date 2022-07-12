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

package io.kyligence.kap.rest.delegate;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.rest.request.AddSegmentRequest;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;
import io.kyligence.kap.rest.request.MergeSegmentRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Set;

@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/models/feign")
public interface ModelMetadataRPC extends ModelMetadataContract {

    @PostMapping(value = "/update_index")
    void updateIndex(@RequestParam("project") String project, @RequestParam("epochId") long epochId,
                     @RequestParam("modelId") String modelId, @RequestBody Set<Long> toBeDeletedLayoutIds,
                     @RequestParam("deleteAuto") boolean deleteAuto, @RequestParam("deleteManual") boolean deleteManual);

    @PostMapping(value = "/update_dataflow")
    void updateDataflow(@RequestBody DataFlowUpdateRequest dataFlowUpdateRequest);

    @PostMapping(value = "/update_index_plan")
    void updateIndexPlan(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
                         @RequestBody IndexPlan indexplan, @RequestParam("action") String action);

    @PostMapping(value = "/update_dataflow_status")
    void updateDataflowStatus(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
                              @RequestParam("status") RealizationStatusEnum status);

    @PostMapping(value = "/get_model_id_by_fuzzy_name")
    List<String> getModelIdsByFuzzyName(@RequestParam("fuzzyName") String fuzzyName,
            @RequestParam("project") String project);

    @PostMapping(value = "/get_model_name_by_id")
    String getModelNameById(@RequestParam("modelId") String modelId, @RequestParam("project") String project);

    @PostMapping(value = "/feign/get_segment_by_range")
    Segments<NDataSegment> getSegmentsByRange(@RequestParam("modelId") String modelId,
            @RequestParam("project") String project, @RequestParam("start") String start,
            @RequestParam("end") String end);

    @PostMapping(value = "/update_second_storage_model")
    String updateSecondStorageModel(@RequestParam("project") String project, @RequestParam("modelId") String modelId);


    @PostMapping(value = "/update_data_model_semantic")
    BuildBaseIndexResponse updateDataModelSemantic(@RequestParam("project") String project,
                                                   @RequestBody ModelRequest request);

    @PostMapping(value = "/save_data_format_if_not_exist")
    void saveDateFormatIfNotExist(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
                                  @RequestParam("format") String format);

    @PostMapping(value = "/append_segment")
    NDataSegment appendSegment(@RequestBody AddSegmentRequest request);

    @PostMapping(value = "/refresh_segment")
    NDataSegment refreshSegment(@RequestParam("project") String project,
                                @RequestParam("indexPlanUuid") String indexPlanUuid, @RequestParam("segmentId") String segmentId);

    @PostMapping(value = "/append_partitions")
    NDataSegment appendPartitions(@RequestParam("project") String project, @RequestParam("dfIF") String dfId,
                                  @RequestParam("segId") String segId, @RequestBody List<String[]> partitionValues);

    @PostMapping(value = "/merge_segments")
    NDataSegment mergeSegments(@RequestParam("project") String project,
                               @RequestBody MergeSegmentRequest mergeSegmentRequest);

    @PostMapping(value = "/delete_segment_by_id")
    void deleteSegmentById(@RequestParam("model") String model, @RequestParam("project") String project,
                           @RequestBody String[] ids, @RequestParam("force") boolean force);

    @PostMapping(value = "/remove_indexes_from_segments")
    void removeIndexesFromSegments(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
                                   @RequestParam("segmentIds") List<String> segmentIds, @RequestParam("indexIds") List<Long> indexIds);
}
