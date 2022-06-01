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

import io.kyligence.kap.job.delegate.JobMetadataDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

@Controller
@RequestMapping(value = "/api/job_delegate", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobMetadataDelegateController {

    @Autowired
    private JobMetadataDelegate jobMetadataDelegate;

    @PostMapping(value = "/feign/add_index_job")
    @ResponseBody
    public String addIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addIndexJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_second_storage_job")
    @ResponseBody
    public String addSecondStorageJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSecondStorageJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_segment_job")
    @ResponseBody
    public String addSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSegmentJob(jobMetadataRequest);
    }
}
