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

package io.kyligence.kap.rest.interceptor;

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_WITHOUT_RESOURCE_GROUP;

import java.io.IOException;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import lombok.val;

@Component
@Order(200)
public class ResourceGroupCheckerFilter implements Filter {
    private static final String ERROR = "error";
    private static final String API_ERROR = "/api/error";

    private static Set<String> notCheckGetApiSet = Sets.newHashSet();
    private static Set<String> notCheckSpecialApiSet = Sets.newHashSet();

    static {
        notCheckSpecialApiSet.add("/kylin/api/error");

        // list projects
        notCheckGetApiSet.add("/kylin/api/projects");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if(!(request instanceof HttpServletRequest)){
            return;
        }

        if (checkRequestPass((HttpServletRequest) request)) {
            chain.doFilter(request, response);
            return;
        }

        // project is not bound to resource group
        if (!checkProjectBindToResourceGroup((HttpServletRequest) request)) {
            Message msg = MsgPicker.getMsg();
            request.setAttribute(ERROR, new KylinException(PROJECT_WITHOUT_RESOURCE_GROUP, msg.getPROJECT_WITHOUT_RESOURCE_GROUP()));
            request.getRequestDispatcher(API_ERROR).forward(request, response);
            return;
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // just override it
    }

    private boolean checkProjectBindToResourceGroup(HttpServletRequest request) {
        Pair<String, HttpServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(request);
        String project = projectInfo.getFirst();

        val manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (UnitOfWork.GLOBAL_UNIT.equals(project) || !manager.isResourceGroupEnabled()) {
            return true;
        }
        return manager.isProjectBindToResourceGroup(project);
    }

    private boolean checkRequestPass(HttpServletRequest request) {
        final String uri = StringUtils.stripEnd(request.getRequestURI(), "/");
        final String method = request.getMethod();

        boolean whitelistOfGetApi =  "GET".equals(method) && notCheckGetApiSet.contains(uri);

        return notCheckSpecialApiSet.contains(uri) || whitelistOfGetApi;
    }

}
