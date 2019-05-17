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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.rest.request.CSVRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.source.file.CheckCredentialException;
import io.kyligence.kap.source.file.CheckObjectException;
import io.kyligence.kap.source.file.CredentialOperator;

@Component("fileSourceService")
public class FileSourceService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FileSourceService.class);

    @Autowired
    private ProjectService projectService;

    @Autowired
    private TableExtService tableExtService;

    public LoadTableResponse saveCSV(String mode, CSVRequest csvRequest) throws Exception {
        String sql;
        SparkSession ss = SparderEnv.getSparkSession();

        // generate ddl
        CredentialOperator credentialOperator = CredentialOperator.chooseCredential(csvRequest.getType());
        Preconditions.checkNotNull(credentialOperator);
        credentialOperator = credentialOperator.prepare(csvRequest.getCredential(), ss);
        if ("expert".equals(mode)) {
            sql = csvRequest.getDdl();
        } else {
            String tableData = csvRequest.getTableData();
            TableDesc tableDesc = JsonUtil.readValue(tableData, TableDesc.class);
            String database;
            if (StringUtils.isEmpty(tableDesc.getDatabase())) {
                database = "default";
            } else {
                database = tableDesc.getDatabase();
            }
            ss.sql("create database if not exists " + database);
            sql = generateCSVSql(tableDesc, csvRequest);
        }

        // create table
        List<String> tableList = createTable(credentialOperator, sql);
        ss.sql("use default"); // in case multiple ddl may change database

        // add credentialOperator
        addCredential(csvRequest.getProject(), credentialOperator);
        Preconditions.checkState(!tableList.isEmpty(), MsgPicker.getMsg().getNoTableFound());
        // load table
        return tableExtService.loadTables(tableList.toArray(new String[tableList.size()]), csvRequest.getProject());
    }

    private List<String> createTable(CredentialOperator credentialOperator, String sql) {
        //for multiple sql
        String[] sqls = sql.split(";");
        List<String> tableList = Lists.newArrayList();
        SparkSession ss = SparderEnv.getSparkSession();
        for (String s : sqls) {
            String table = findTableName(s);
            s = credentialOperator.massageUrl(s);
            ss.sql(s);
            if (table != null) {
                tableList.add(table);
            }
        }
        return tableList;
    }

    public String generateCSVSql(TableDesc tableDesc, CSVRequest csvRequest) {
        CredentialOperator credentialOperator = CredentialOperator.chooseCredential(csvRequest.getType());
        Preconditions.checkNotNull(credentialOperator, "CredentialOperator can not be null");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("create external table ");
        stringBuilder.append(tableDesc.getDatabase().toUpperCase()).append(".")
                .append(tableDesc.getName().toUpperCase());
        stringBuilder.append("(");
        for (ColumnDesc columnDesc : tableDesc.getColumns()) {
            stringBuilder.append(columnDesc.getName()).append(" ").append(columnDesc.getDatatype()).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(") ");
        stringBuilder.append("row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties(");
        stringBuilder.append("\"separatorChar\" = \"").append(escape(csvRequest.getSeparatorChar())).append("\", ");
        stringBuilder.append("\"quoteChar\" = \"").append(escape(csvRequest.getQuoteChar())).append("\")");
        stringBuilder.append(" location '");
        stringBuilder.append(credentialOperator.massageUrl(csvRequest.getUrl())).append("'");
        return stringBuilder.toString();
    }

    public String escape(String s) {
        if (s.contains("\"")) {
            s = s.replace("\"", "\\\"");
        }
        return s;
    }

    public void addCredential(String project, CredentialOperator credentialOperator) {
        projectService.updateFileSourceCredential(project, credentialOperator);
    }

    public String findTableName(String sql) {
        String ddlPrefix = "CREATEEXTERNALTABLE";
        sql = sql.trim().toUpperCase().replaceAll("\\s*", "");
        if (sql.startsWith(ddlPrefix)) {
            return sql.substring(ddlPrefix.length(), sql.indexOf('('));
        }
        return null;
    }

    public boolean verifyCredential(CSVRequest csvRequest) {
        CredentialOperator credentialOperator = CredentialOperator.chooseCredential(csvRequest.getType());
        Preconditions.checkNotNull(credentialOperator);
        credentialOperator = credentialOperator.decode(csvRequest.getCredential());
        Message msg = MsgPicker.getMsg();
        try {
            credentialOperator.verify(csvRequest.getUrl());
        } catch (CheckCredentialException e1) {
            throw new IllegalStateException(msg.getINVALID_CREDENTIAL());
        } catch (CheckObjectException e2) {
            throw new IllegalStateException(msg.getINVALID_URL());
        }
        return true;
    }

    public String[][] csvSamples(CSVRequest csvRequest) {
        CredentialOperator credentialOperator = CredentialOperator.chooseCredential(csvRequest.getType());
        Preconditions.checkNotNull(credentialOperator);
        credentialOperator = credentialOperator.prepare(csvRequest.getCredential(), SparderEnv.getSparkSession());
        Map<String, String> map = Maps.newHashMap();
        map.put("delimiter", csvRequest.getSeparatorChar());
        map.put("quote", csvRequest.getQuoteChar());
        map.put("escape", csvRequest.getEscapeChar());
        map.put("inferSchema", "false");
        return credentialOperator.csvSamples(csvRequest.getUrl(), map);
    }

    public List<String> csvSchema(CSVRequest csvRequest) {
        CredentialOperator credentialOperator = CredentialOperator.chooseCredential(csvRequest.getType());
        Preconditions.checkNotNull(credentialOperator);
        credentialOperator = credentialOperator.prepare(csvRequest.getCredential(), SparderEnv.getSparkSession());
        Map<String, String> map = Maps.newHashMap();
        map.put("delimiter", csvRequest.getSeparatorChar());
        map.put("quote", csvRequest.getQuoteChar());
        map.put("escape", csvRequest.getEscapeChar());
        map.put("header", "true"); //skip infer header
        return credentialOperator.csvSchema(csvRequest.getUrl(), map);
    }

    public boolean validateSql(String ddl) throws Exception {
        try {
            String[] sqls = ddl.split(";");
            for (String sql : sqls) {
                SparderEnv.validateSql(sql);
            }
        } catch (Exception exception) {
            logger.error("validate sql failed. {}", exception.getMessage());
            throw exception;
        }
        return true;
    }

}
