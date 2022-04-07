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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.response.ImportSqlResponse;
import io.kyligence.kap.rest.response.SQLParserResponse;
import io.kyligence.kap.rest.response.SQLValidateResponse;
import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.kyligence.kap.smart.query.validator.SqlSyntaxValidator;

@Component("favoriteRuleService")
public class FavoriteRuleService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleService.class);

    private static final String DEFAULT_SCHEMA = "DEFAULT";

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, SQLValidateResult> batchSqlValidate(List<String> sqls, String project) {
        KylinConfig kylinConfig = getProjectManager().getProject(project).getConfig();
        AbstractSQLValidator sqlValidator = new SqlSyntaxValidator(project, kylinConfig);
        return sqlValidator.batchValidate(sqls.toArray(new String[0]));
    }

    public SQLParserResponse importSqls(MultipartFile[] files, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> sqls = Lists.newArrayList();
        List<String> wrongFormatFiles = Lists.newArrayList();

        // add user info before invoking row-filter and hack-select-star
        QueryContext context = QueryContext.current();
        context.setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
        // parse file to sqls
        for (MultipartFile file : files) {
            String fileName = file.getOriginalFilename();
            if (!StringUtils.endsWithIgnoreCase(fileName, ".sql")
                    && !StringUtils.endsWithIgnoreCase(fileName, ".txt")) {
                wrongFormatFiles.add(file.getOriginalFilename());
                continue;
            }
            try {
                sqls.addAll(transformFileToSqls(file, project));
            } catch (Exception | Error ex) {
                logger.error("[UNEXPECTED_THINGS_HAPPENED]Error caught when parsing file {} because {} ",
                        file.getOriginalFilename(), ex);
                wrongFormatFiles.add(file.getOriginalFilename());
            }
        }
        SQLParserResponse result = new SQLParserResponse();
        KylinConfig kylinConfig = getProjectManager().getProject(project).getConfig();
        if (sqls.size() > kylinConfig.getFavoriteImportSqlMaxSize()) {
            result.setSize(sqls.size());
            result.setWrongFormatFile(wrongFormatFiles);
            return result;
        }

        List<ImportSqlResponse> sqlData = Lists.newArrayList();
        int capableSqlNum = 0;

        //sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(sqls, project);
        int id = 0;

        for (Map.Entry<String, SQLValidateResult> entry : map.entrySet()) {
            String sql = entry.getKey();
            SQLValidateResult validateResult = entry.getValue();

            if (validateResult.isCapable())
                capableSqlNum++;

            ImportSqlResponse sqlResponse = new ImportSqlResponse(sql, validateResult.isCapable());
            sqlResponse.setId(id);
            sqlResponse.setSqlAdvices(validateResult.getSqlAdvices());
            sqlData.add(sqlResponse);

            id++;
        }

        // make sql grammar failed sqls ordered first
        sqlData.sort((object1, object2) -> {
            boolean capable1 = object1.isCapable();
            boolean capable2 = object2.isCapable();

            if (capable1 && !capable2)
                return 1;

            if (capable2 && !capable1)
                return -1;

            return 0;
        });

        result.setData(sqlData);
        result.setSize(sqlData.size());
        result.setCapableSqlNum(capableSqlNum);
        result.setWrongFormatFile(wrongFormatFiles);

        return result;
    }

    List<String> transformFileToSqls(MultipartFile file, String project) throws IOException {
        List<String> sqls = new ArrayList<>();

        String content = new String(file.getBytes(), StandardCharsets.UTF_8);

        if (content.isEmpty()) {
            return sqls;
        }
        content = QueryUtil.removeCommentInSql(content);
        List<String> sqlLists = QueryUtil.splitBySemicolon(content);
        if (sqlLists.isEmpty()) {
            return sqls;
        }
        KylinConfig kylinConfig = getProjectManager().getProject(project).getConfig();
        for (String sql : sqlLists) {
            if (sql == null || sql.length() == 0 || sql.replace('\n', ' ').trim().length() == 0) {
                continue;
            }
            QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, DEFAULT_SCHEMA, false);
            queryParams.setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
            // massage sql and expand CC columns
            String correctedSql = QueryUtil.massageSqlAndExpandCC(queryParams);
            sqls.add(correctedSql);
        }

        return sqls;
    }

    public SQLValidateResponse sqlValidate(String project, String sql) {
        aclEvaluate.checkProjectWritePermission(project);
        KylinConfig kylinConfig = getProjectManager().getProject(project).getConfig();
        QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, DEFAULT_SCHEMA, false);
        queryParams.setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(project, getCurrentUserGroups()));
        String correctedSql = QueryUtil.massageSql(queryParams);
        // sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(Lists.newArrayList(correctedSql), project);
        SQLValidateResult result = map.get(correctedSql);

        return new SQLValidateResponse(result.isCapable(), result.getSqlAdvices());
    }
}
