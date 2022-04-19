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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.JOB_NODE_API_INVALID;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.Sets;

import io.kyligence.kap.rest.cluster.ClusterManager;
import lombok.extern.slf4j.Slf4j;

/**
 **/
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class JobNodeFilter implements Filter {
    private static final String ERROR = "error";
    private static final String API_ERROR = "/api/error";
    private static final String FILTER_PASS = "filter_pass";
    private static Set<String> jobNodeAbandonApiSet = Sets.newHashSet();

    static {
        //jobNode abandon url
        jobNodeAbandonApiSet.add("/data_range/latest_data");
        jobNodeAbandonApiSet.add("/kylin/api/tables/partition_column_format");
    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ClusterManager clusterManager;

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (!(servletRequest instanceof HttpServletRequest)) {
            return;
        }
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (checkJobNodeAbandon(kylinConfig, request, response, chain)) {
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean checkJobNodeAbandon(KylinConfig kylinConfig, HttpServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {

        boolean isJobNodePass = jobNodeAbandonApiSet.stream().filter(request.getRequestURI()::contains)
                .collect(Collectors.toList()).isEmpty();
        if (!isJobNodePass) {
            if (kylinConfig.isQueryNode()) {
                request.setAttribute(FILTER_PASS, "true");
                chain.doFilter(request, response);
            } else {
                request.setAttribute(ERROR, new KylinException(JOB_NODE_API_INVALID));
                request.getRequestDispatcher(API_ERROR).forward(request, response);
            }
            return true;
        }
        return false;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init query request filter");
        // just override it
    }

    @Override
    public void destroy() {
        // just override it
    }
}
