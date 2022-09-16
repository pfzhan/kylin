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

package org.apache.kylin.rest.feign;

import java.util.List;

import org.apache.kylin.job.execution.DumpInfo;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/models/feign")
public interface MetadataRPC extends MetadataContract{

    @PostMapping(value = "/merge_metadata")
    List<NDataLayout[]> mergeMetadata(@RequestParam("project") String project, @RequestBody MergerInfo mergerInfo);

    @PostMapping(value = "/make_segment_ready")
    void makeSegmentReady(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("segmentId") String segmentId,
            @RequestParam("errorOrPausedJobCount") int errorOrPausedJobCount);

    @PostMapping(value = "/attach_metadata_and_kylin_props")
    void attachMetadataAndKylinProps(@RequestParam("project") String project, @RequestBody DumpInfo dumpInfo);
}
