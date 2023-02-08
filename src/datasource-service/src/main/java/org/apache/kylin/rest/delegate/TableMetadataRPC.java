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

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.aspect.WaitForSyncAfterRPC;
import org.apache.kylin.rest.request.MergeAndUpdateTableExtRequest;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@EnableFeignClients
@FeignClient(name = "common", path = "/kylin/api/tables/feign")
public interface TableMetadataRPC extends TableMetadataContract {

    @PostMapping(value = "/merge_and_update_table_ext")
    @WaitForSyncAfterRPC
    void mergeAndUpdateTableExt(@RequestParam("project") String project,
            @RequestBody MergeAndUpdateTableExtRequest request);

    @PostMapping(value = "/save_table_ext")
    @WaitForSyncAfterRPC
    void saveTableExt(@RequestParam("project") String project, @RequestBody TableExtDesc tableExt);

    @PostMapping(value = "/update_table_desc")
    @WaitForSyncAfterRPC
    void updateTableDesc(@RequestParam("project") String project, @RequestBody TableDesc tableDesc);

    @GetMapping("/get_table_names_by_fuzzy_key")
    List<String> getTableNamesByFuzzyKey(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "fuzzyKey") String fuzzyKey);
}
