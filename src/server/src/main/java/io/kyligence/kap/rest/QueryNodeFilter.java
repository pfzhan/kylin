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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CONNECT_CATALOG;
import static org.apache.kylin.common.exception.ServerErrorCode.NO_ACTIVE_ALL_NODE;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_WITHOUT_RESOURCE_GROUP;
import static org.apache.kylin.common.exception.ServerErrorCode.SYSTEM_IS_RECOVER;
import static org.apache.kylin.common.exception.ServerErrorCode.TRANSFER_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.MAINTENANCE_MODE_WRITE_FAILED;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
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
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.interceptor.ProjectInfoParser;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 3)
public class QueryNodeFilter implements Filter {

    private static final String API_PREFIX = "/kylin/api";
    private static final String ROUTED = "routed";
    private static final String ERROR = "error";
    private static final String API_ERROR = "/api/error";
    private static final String FILTER_PASS = "filter_pass";

    private static Set<String> routeGetApiSet = Sets.newHashSet();
    private static Set<String> notRoutePostApiSet = Sets.newHashSet();
    private static Set<String> notRouteDeleteApiSet = Sets.newHashSet();
    private static Set<String> notRoutePutApiSet = Sets.newHashSet();

    private static String ERROR_REQUEST_URL = "/kylin/api/error";

    static {
        // data source
        routeGetApiSet.add("/kylin/api/tables/reload_hive_table_name");
        routeGetApiSet.add("/kylin/api/tables/project_table_names");
        routeGetApiSet.add("/kylin/api/query/favorite_queries");

        // jdbc, odbc, query, maintain
        notRoutePostApiSet.add("/kylin/api/query");
        notRoutePostApiSet.add("/kylin/api/async_query");
        notRoutePostApiSet.add("/kylin/api/query/prestate");
        notRoutePostApiSet.add("/kylin/api/user/authentication");
        notRoutePostApiSet.add("/kylin/api/system/maintenance_mode");
        notRouteDeleteApiSet.add("/kylin/api/query");

        notRoutePostApiSet.add("/kylin/api/health/instance_info");
        notRoutePostApiSet.add("/kylin/api/health/instance_service/query_up_grade");
        notRoutePostApiSet.add("/kylin/api/health/instance_service/query_down_grade");

        // license
        notRoutePostApiSet.add("/kylin/api/system/license/content");
        notRoutePostApiSet.add("/kylin/api/system/license/file");

        //diag
        notRoutePostApiSet.add("/kylin/api/system/diag");
        notRouteDeleteApiSet.add("/kylin/api/system/diag");
        notRouteDeleteApiSet.add("/kylin/api/system/maintenance_mode");
        notRoutePutApiSet.add("/kylin/api/system/diag/progress");

        //download
        notRoutePostApiSet.add("/kylin/api/metastore/backup/models");
        notRoutePostApiSet.add("/kylin/api/query/format/csv");

        //refresh catalog
        notRoutePutApiSet.add("/kylin/api/tables/catalog_cache");
        notRoutePutApiSet.add("/kylin/api/tables/single_catalog_cache");
        notRoutePutApiSet.add("/kylin/api/index_plans/agg_index_count");
        notRoutePutApiSet.add("/kylin/api/system/roll_event_log");

        //epoch
        notRoutePostApiSet.add("/kylin/api/epoch");
        notRoutePostApiSet.add("/kylin/api/epoch/all");

        //reload metadata
        notRoutePostApiSet.add("/kylin/api/system/metadata/reload");

        //second storage
        routeGetApiSet.add("/kylin/api/storage/table/sync");
        notRoutePostApiSet.add("/kylin/api/storage/config/refresh");
        notRoutePostApiSet.add("/kylin/api/storage/node/status");
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
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String project;
            try {
                // not start with /kylin/api
                if (checkNeedToRoute(servletRequest)) {
                    chain.doFilter(request, response);
                    return;
                }

                // no leaders
                if (CollectionUtils.isEmpty(clusterManager.getJobServers())) {
                    Message msg = MsgPicker.getMsg();
                    servletRequest.setAttribute(ERROR,
                            new KylinException(NO_ACTIVE_ALL_NODE, msg.getNO_ACTIVE_LEADERS()));
                    servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                    return;
                }

                String contentType = request.getContentType();
                Pair<String, HttpServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(servletRequest);
                project = projectInfo.getFirst();
                if (!checkProjectExist(project)) {
                    servletRequest.setAttribute(ERROR, new KylinException(PROJECT_NOT_EXIST, project));
                    servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                    return;
                }

                request = projectInfo.getSecond();

                if (checkProcessLocal(kylinConfig, project, contentType)) {
                    log.info("process local caused by project owner");
                    chain.doFilter(request, response);
                    return;
                }

                if (EpochManager.getInstance().isMaintenanceMode()) {
                    servletRequest.setAttribute(ERROR, new KylinException(MAINTENANCE_MODE_WRITE_FAILED));
                    servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                    return;
                }
            } catch (CannotCreateTransactionException e) {
                writeConnectionErrorResponse(servletRequest, servletResponse);
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
            ErrorCode.setMsg(servletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
            try {
                val exchange = restTemplate.exchange(
                        "http://all" + servletRequest.getRequestURI() + "?" + servletRequest.getQueryString(),
                        HttpMethod.valueOf(servletRequest.getMethod()), new HttpEntity<>(body, headers), byte[].class);
                tryCatchUp();
                responseHeaders = exchange.getHeaders();
                responseBody = Optional.ofNullable(exchange.getBody()).orElse(new byte[0]);
                responseStatus = exchange.getStatusCodeValue();
            } catch (IllegalStateException | ResourceAccessException e) {
                responseStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
                Message msg = MsgPicker.getMsg();
                KylinException exception;
                val manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
                if (manager.isResourceGroupEnabled() && !manager.isProjectBindToResourceGroup(project)) {
                    exception = new KylinException(PROJECT_WITHOUT_RESOURCE_GROUP,
                            msg.getPROJECT_WITHOUT_RESOURCE_GROUP());
                } else {
                    exception = new KylinException(SYSTEM_IS_RECOVER, msg.getLEADERS_HANDLE_OVER());
                }
                ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(servletRequest),
                        exception);
                responseBody = JsonUtil.writeValueAsBytes(errorResponse);
                responseHeaders = new HttpHeaders();
                responseHeaders.setContentType(MediaType.APPLICATION_JSON);
                log.error("no job node", e);
            } catch (HttpStatusCodeException e) {
                responseStatus = e.getRawStatusCode();
                responseBody = e.getResponseBodyAsByteArray();
                responseHeaders = Optional.ofNullable(e.getResponseHeaders()).orElse(new HttpHeaders());
                log.warn("code {}, error {}", e.getStatusCode(), e.getMessage());
            } catch (Exception e) {
                log.error("transfer failed", e);
                servletRequest.setAttribute(ERROR,
                        new KylinException(TRANSFER_FAILED, MsgPicker.getMsg().getTRANSFER_FAILED()));
                servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                return;
            }
            servletResponse.setStatus(responseStatus);
            setResponseHeaders(responseHeaders, servletResponse);
            servletResponse.getOutputStream().write(responseBody);
            return;
        }
        throw new RuntimeException("unknown status");
    }

    private void setResponseHeaders(HttpHeaders responseHeaders, HttpServletResponse servletResponse) {
        responseHeaders.forEach((k, v) -> {
            if (k.equals(HttpHeaders.TRANSFER_ENCODING)) {
                return;
            }
            for (String headerValue : v) {
                servletResponse.setHeader(k, headerValue);
            }
        });
    }

    private void tryCatchUp() {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.getAuditLogStore().catchupWithTimeout();
        } catch (Exception e) {
            log.error("Failed to catchup manually.", e);
        }
    }

    @Override
    public void destroy() {
        // just override it
    }

    private boolean checkNeedToRoute(HttpServletRequest servletRequest) {
        final String uri = StringUtils.stripEnd(servletRequest.getRequestURI(), "/");
        final String method = servletRequest.getMethod();
        return (!uri.startsWith(API_PREFIX)) || (uri.startsWith(ERROR_REQUEST_URL))
                || (method.equals("GET") && !routeGetApiSet.contains(uri))
                || (method.equals("POST") && notRoutePostApiSet.contains(uri))
                || (method.equals("PUT") && notRoutePutApiSet.contains(uri))
                || (method.equals("DELETE") && notRouteDeleteApiSet.contains(uri))
                || "true".equalsIgnoreCase(servletRequest.getHeader(ROUTED))
                || "true".equals(servletRequest.getAttribute(FILTER_PASS))
                || KylinConfig.getInstanceFromEnv().isUTEnv();
    }

    private boolean checkProcessLocal(KylinConfig kylinConfig, String project, String contentType) {
        if (kylinConfig.isQueryNodeOnly()) {
            return false;
        }

        if (!EpochManager.getInstance().checkEpochOwner(project)) {
            return false;
        }

        return StringUtils.isEmpty(contentType) || !contentType.contains("multipart/form-data");
    }

    private boolean checkProjectExist(String project) {
        if (!UnitOfWork.GLOBAL_UNIT.equals(project)) {
            val prj = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
            if (prj == null) {
                return false;
            }
        }
        return true;
    }

    public void writeConnectionErrorResponse(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
            throws IOException {
        ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(servletRequest),
                new KylinException(FAILED_CONNECT_CATALOG, MsgPicker.getMsg().getCONNECT_DATABASE_ERROR(), false));
        byte[] responseBody = JsonUtil.writeValueAsBytes(errorResponse);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_JSON);
        servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        setResponseHeaders(responseHeaders, servletResponse);
        servletResponse.getOutputStream().write(responseBody);
    }
}
