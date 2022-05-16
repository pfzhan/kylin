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

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.rest.request.AddSegmentRequest;
import io.kyligence.kap.rest.request.MergeSegmentRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.metadata.model.Segments;

import java.util.List;

public interface ModelMetadataContract {

    List<String> getModelIdsByFuzzyName(String fuzzyName, String project);

    String getModelNameById(String modelId, String project);

    Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end);

    String updateSecondStorageModel(String project, String modelId);

    BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request);

    void saveDateFormatIfNotExist(String project, String modelId, String format);

    NDataSegment appendSegment(AddSegmentRequest request);

    NDataSegment refreshSegment(String project, String indexPlanUuid, String segmentId);

    NDataSegment appendPartitions(String project, String dfId, String segId, List<String[]> partitionValues);

    NDataSegment mergeSegments(String project, MergeSegmentRequest mergeSegmentRequest);

    void deleteSegmentById(String model, String project, String[] ids, boolean force);

    void removeIndexesFromSegments(String project, String modelId, List<String> segmentIds,
                                   List<Long> indexIds);
}
