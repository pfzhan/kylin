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

import com.google.common.base.Preconditions;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService {
    public HashMap<String, Object> getQueryHistories(QueryHistoryRequest request, final int limit, final int offset) {
        Preconditions.checkArgument(request.getProject() != null && StringUtils.isNotEmpty(request.getProject()));
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao(request.getProject());

        HashMap<String, Object> data = new HashMap<>();
        data.put("query_histories", queryHistoryDAO.getQueryHistoriesByConditions(request, limit, offset));
        data.put("size", queryHistoryDAO.getQueryHistoriesSize(request));

        return data;
    }
}
