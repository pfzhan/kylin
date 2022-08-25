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

import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@EnableFeignClients
@FeignClient(name = "yinglong-data-loading-booter", path = "/kylin/api/tables/feign")
public interface TableSamplingRPC extends TableSamplingContract {

    @PostMapping(value = "/sampling")
    List<String> sampling(@RequestBody Set<String> tables, @RequestParam("project") String project,
            @RequestParam("rows") int rows, @RequestParam("priority") int priority,
            @RequestParam("yarnQueue") String yarnQueue, @RequestParam("tag") Object tag);
}
