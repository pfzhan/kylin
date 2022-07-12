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
import org.apache.kylin.metadata.model.Segments;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/models/feign")
public interface ModelMetadataRPC extends ModelMetadataContract {

    @PostMapping(value = "/get_model_id_by_fuzzy_name")
    List<String> getModelIdsByFuzzyName(@RequestParam("fuzzyName") String fuzzyName,
            @RequestParam("project") String project);

    @PostMapping(value = "/get_model_name_by_id")
    String getModelNameById(@RequestParam("modelId") String modelId, @RequestParam("project") String project);

    @PostMapping(value = "/feign/get_segment_by_range")
    Segments<NDataSegment> getSegmentsByRange(@RequestParam("modelId") String modelId,
            @RequestParam("project") String project, @RequestParam("start") String start,
            @RequestParam("end") String end);

}
