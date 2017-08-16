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

package io.kyligence.kap.rest;

import javax.servlet.ServletContextEvent;

import org.apache.kylin.common.KapConfig;

public class Log4jConfigListener extends org.springframework.web.util.Log4jConfigListener {

    private boolean isDebugTomcat;

    public Log4jConfigListener() {
        KapConfig config = KapConfig.getInstanceFromEnv();
        this.isDebugTomcat = config.isDevEnv();

    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        if (!isDebugTomcat) {
            super.contextInitialized(event);
        }
        LicenseGatherUtil.gatherLicenseInfo(LicenseGatherUtil.getDefaultLicenseFile(),
                LicenseGatherUtil.getDefaultCommitFile(), LicenseGatherUtil.getDefaultVersionFile(), null);
        System.setProperty("needCheckCC", "true");
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        if (!isDebugTomcat) {
            super.contextDestroyed(event);
        }
    }

}
