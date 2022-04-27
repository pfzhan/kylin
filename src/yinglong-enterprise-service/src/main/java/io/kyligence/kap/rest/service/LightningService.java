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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.SYSTEM_PROFILE_ABNORMAL_DATA;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.SystemProfileExtractorFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import io.kyligence.kap.rest.response.SystemProfileResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("lightningService")
public class LightningService extends BasicService {

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public SystemProfileResponse systemProfile() {

        if (!KapConfig.getInstanceFromEnv().isCloud()) {
            SystemProfileResponse systemProfileResponse = new SystemProfileResponse();
            systemProfileResponse.setIsCloud(false);
            return systemProfileResponse;
        }

        try {
            String data = SystemProfileExtractorFactory.create(KylinConfig.getInstanceFromEnv()).getSystemProfile();
            return JsonUtil.readValue(data, SystemProfileResponse.class);
        } catch (Exception e) {
            throw new KylinException(SYSTEM_PROFILE_ABNORMAL_DATA, e);
        }
    }

}
