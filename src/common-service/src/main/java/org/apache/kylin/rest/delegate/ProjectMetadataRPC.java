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

import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.rest.aspect.WaitForSyncAfterRPC;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@EnableFeignClients
@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/projects/feign")
public interface ProjectMetadataRPC extends ProjectMetadataContract {
    @PostMapping(value = "/update_rule")
    @WaitForSyncAfterRPC
    void updateRule(@RequestBody List<FavoriteRule.AbstractCondition> conditions,
            @RequestParam("isEnabled") boolean isEnabled, @RequestParam("ruleName") String ruleName,
            @RequestParam("project") String project);

    @PostMapping(value = "/save_async_task")
    @WaitForSyncAfterRPC
    void saveAsyncTask(@RequestParam("project") String project, @RequestBody AsyncAccelerationTask task);
}
