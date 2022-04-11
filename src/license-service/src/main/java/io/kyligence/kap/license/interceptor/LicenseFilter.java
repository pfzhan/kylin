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
package io.kyligence.kap.license.interceptor;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.kyligence.kap.license.service.LicenseInfoService;
import lombok.val;

@Component
@Order(-500)
public class LicenseFilter implements Filter {

    private static final String PREFIX = "/kylin/api/";
    private static String[] apiWhiteList = { "config", "system", "error", "health", "prometheus" };

    @Autowired
    private LicenseInfoService licenseInfoService;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            if (servletRequest.getRequestURI().startsWith(PREFIX)) {
                boolean noNeedLicenseCheck = Arrays.stream(apiWhiteList).map(white -> PREFIX + white)
                        .anyMatch(api -> servletRequest.getRequestURI().startsWith(api));
                if (!noNeedLicenseCheck) {
                    try {
                        LicenseInfoService.licenseReadWriteLock.readLock().lock();
                        val info = licenseInfoService.extractLicenseInfo();
                        licenseInfoService.verifyLicense(info);
                    } catch (Exception e) {
                        if (e instanceof KylinException) {
                            servletRequest.setAttribute("error", e);
                        } else {
                            servletRequest.setAttribute("error", new UnauthorizedException(USER_AUTH_INFO_NOTFOUND));
                        }
                        servletRequest.getRequestDispatcher("/api/error").forward(servletRequest, response);
                        return;
                    } finally {
                        LicenseInfoService.licenseReadWriteLock.readLock().unlock();
                    }
                }
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // just override it
    }
}
