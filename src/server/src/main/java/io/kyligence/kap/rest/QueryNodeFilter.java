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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.interceptor.ProjectInfoParser;
import lombok.val;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class QueryNodeFilter implements Filter {

    private static final String API_PREFIX = "/kylin/api";
    private static final String ROUTED = "routed";


    private static Set<String> routeGetApiSet = Sets.newHashSet();
    private static Set<String> notRoutePostApiSet = Sets.newHashSet();
    private static Set<String> notRouteDeleteApiSet = Sets.newHashSet();
    private static String ERROR_REQUEST_URL = "/kylin/api/error";


    static {
        // data source
        routeGetApiSet.add("/kylin/api/tables/reload_hive_table_name");
        routeGetApiSet.add("/kylin/api/tables/project_table_names");
        routeGetApiSet.add("/kylin/api/query/favorite_queries");

        // jdbc, odbc, query
        notRoutePostApiSet.add("/kylin/api/query");
        notRoutePostApiSet.add("/kylin/api/query/prestate");
        notRoutePostApiSet.add("/kylin/api/user/authentication");

        // license
        notRoutePostApiSet.add("/kylin/api/system/license/content");
        notRoutePostApiSet.add("/kylin/api/system/license/file");

        //diag
        notRoutePostApiSet.add("/kylin/api/system/diag");
        notRouteDeleteApiSet.add("/kylin/api/system/diag");

        //download
        notRoutePostApiSet.add("/kylin/api/metastore/backup/models");
        notRoutePostApiSet.add("/kylin/api/query/format/csv");
    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ClusterManager clusterManager;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init query request filter");
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            HttpServletResponse servletResponse = (HttpServletResponse) response;
            String serverMode = KylinConfig.getInstanceFromEnv().getServerMode();
            // not start with /kylin/api
            if (!servletRequest.getRequestURI().startsWith(API_PREFIX)) {
                chain.doFilter(request, response);
                return;
            }
            if (servletRequest.getRequestURI().startsWith(ERROR_REQUEST_URL)) {
                chain.doFilter(request, response);
                return;
            }
            // start with /kylin/api
            if (servletRequest.getMethod().equals("GET") && !routeGetApiSet.contains(servletRequest.getRequestURI())) {
                chain.doFilter(request, response);
                return;
            }
            if (servletRequest.getMethod().equals("POST")
                    && notRoutePostApiSet.contains(servletRequest.getRequestURI())) {
                chain.doFilter(request, response);
                return;
            }
            if (servletRequest.getMethod().equals("DELETE")
                    && notRouteDeleteApiSet.contains(servletRequest.getRequestURI())) {
                chain.doFilter(request, response);
                return;
            }

            // no leaders
            if (CollectionUtils.isEmpty(clusterManager.getJobServers())) {
                Message msg = MsgPicker.getMsg();
                servletRequest.setAttribute("error", new KylinException("KE-4017", msg.getNO_ACTIVE_LEADERS()));
                servletRequest.getRequestDispatcher("/api/error").forward(servletRequest, response);
                return;
            }

            Pair<String, ServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(request);
            String project = projectInfo.getFirst();
            request = projectInfo.getSecond();
            if (Constant.SERVER_MODE_JOB.equalsIgnoreCase(serverMode)
                    || Constant.SERVER_MODE_ALL.equalsIgnoreCase(serverMode)) {
                // process local
                if (((HttpServletRequest) request).getRequestURI().contains("epoch")
                        || EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).checkEpochOwner(project)) {
                    log.info("process local caused by project owner");
                    chain.doFilter(request, response);
                    return;
                }
            }

            if ("true".equalsIgnoreCase(servletRequest.getHeader(ROUTED))) {
                log.info("process local caused by routed once.");
                chain.doFilter(request, response);
                return;
            }

            ServletRequestAttributes attributes = new ServletRequestAttributes((HttpServletRequest) request);
            RequestContextHolder.setRequestAttributes(attributes);

            log.debug("proxy {} {} to all", servletRequest.getMethod(), servletRequest.getRequestURI());
            val body = IOUtils.toByteArray(request.getInputStream());
            HttpHeaders headers = new HttpHeaders();
            Collections.list(servletRequest.getHeaderNames())
                    .forEach(k -> headers.put(k, Collections.list(servletRequest.getHeaders(k))));
            headers.add(ROUTED, "true");
            byte[] responseBody;
            int responseStatus;
            HttpHeaders responseHeaders;
            MsgPicker.setMsg(servletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
            KylinException.setMsg(servletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
            try {
                val exchange = restTemplate.exchange(
                        "http://all" + servletRequest.getRequestURI() + "?" + servletRequest.getQueryString(),
                        HttpMethod.valueOf(servletRequest.getMethod()), new HttpEntity<>(body, headers), byte[].class);
                responseHeaders = exchange.getHeaders();
                responseBody = exchange.getBody();
                responseStatus = exchange.getStatusCodeValue();
            } catch (IllegalStateException e) {
                responseStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
                Message msg = MsgPicker.getMsg();
                ErrorResponse errorResponse = new ErrorResponse(servletRequest.getRequestURL().toString(),
                        new InternalErrorException(msg.getNoJobNode(), e));
                responseBody = JsonUtil.writeValueAsBytes(errorResponse);
                responseHeaders = new HttpHeaders();
                responseHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
                log.error("no job node", e);
            } catch (HttpStatusCodeException e) {
                responseStatus = e.getRawStatusCode();
                responseBody = e.getResponseBodyAsByteArray();
                responseHeaders = e.getResponseHeaders();
                log.warn("code {}, error {}", e.getStatusCode(), e.getMessage());
            } catch (Exception e) {
                log.error("transfer failed", e);
                servletRequest.setAttribute("error", new KylinException("KE-5005", MsgPicker.getMsg().getTRANSFER_FAILED()));
                servletRequest.getRequestDispatcher("/api/error").forward(servletRequest, response);
                return;
            }
            servletResponse.setStatus(responseStatus);
            responseHeaders.forEach((k, v) -> {
                if (k.equals(HttpHeaders.TRANSFER_ENCODING)) {
                    return;
                }
                for (String headerValue : v) {
                    servletResponse.setHeader(k, headerValue);
                }
            });
            servletResponse.getOutputStream().write(responseBody);
            return;
        }
        throw new RuntimeException("unknown status");
    }

    @Override
    public void destroy() {
        // just override it
    }
}
