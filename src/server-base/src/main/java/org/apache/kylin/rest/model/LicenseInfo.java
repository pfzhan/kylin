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
package org.apache.kylin.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kylin.rest.service.LicenseInfoService;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class LicenseInfo {

    @JsonProperty(LicenseInfoService.KE_LICENSE_STATEMENT)
    private String statement;

    @JsonProperty(LicenseInfoService.KE_VERSION)
    private String version;

    @JsonProperty(LicenseInfoService.KE_DATES)
    private String dates;

    @JsonProperty(LicenseInfoService.KE_COMMIT)
    private String commit;

    @JsonProperty(LicenseInfoService.KE_LICENSE_ISEVALUATION)
    private boolean isEvaluation = false;

    @JsonProperty(LicenseInfoService.KE_LICENSE_ISCLOUD)
    private boolean isCloud = false;

    @JsonProperty(LicenseInfoService.KE_LICENSE_SERVICEEND)
    private String serviceEnd;

    @JsonProperty(LicenseInfoService.KE_LICENSE_NODES)
    private String nodes;

    @JsonProperty(LicenseInfoService.KE_LICENSE_VOLUME)
    private String volume;

    @JsonProperty(LicenseInfoService.KE_LICENSE_LEVEL)
    private String level;

    @JsonProperty(LicenseInfoService.KE_LICENSE_INFO)
    private String info;

    @JsonProperty(LicenseInfoService.KE_LICENSE_CATEGORY)
    private String category;

    @JsonIgnore
    public boolean isEvaluation() {
        return isEvaluation;
    }

    @JsonIgnore
    public boolean isCloud() {
        return isCloud;
    }
}
