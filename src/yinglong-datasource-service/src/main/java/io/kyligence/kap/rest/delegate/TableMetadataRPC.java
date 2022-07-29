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

import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import io.kyligence.kap.rest.aspect.WaitForSyncAfterRPC;
import io.kyligence.kap.rest.request.MergeAndUpdateTableExtRequest;

@EnableFeignClients
@FeignClient(name = "yinglong-common-booter", path = "/kylin/api/tables/feign")
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
    List<String> getTableNamesByFuzzyKey(@RequestParam(value = "project") String project,
            @RequestParam(value = "fuzzyKey") String fuzzyKey);
}
