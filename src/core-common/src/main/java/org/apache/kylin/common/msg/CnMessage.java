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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.msg;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.Singletons;

public class CnMessage extends Message {

    protected CnMessage() {

    }

    public static CnMessage getInstance() {
        return Singletons.getInstance(CnMessage.class);
    }

    // Cube
    @Override
    public String getCHECK_CC_AMBIGUITY() {
        return "可计算列名 “%s” 在当前模型下已存在。请重新命名。";
    }

    @Override
    public String getSEG_NOT_FOUND() {
        return "Segment “%s” 在模型 “%s” 内不存在。请重试。";
    }

    @Override
    public String getACL_INFO_NOT_FOUND() {
        return "找不到对象 '%s' 的授权信息";
    }

    @Override
    public String getACL_DOMAIN_NOT_FOUND() {
        return "由于未知对象，当前无法赋权。请稍后再试，或联系技术支持。";
    }

    @Override
    public String getPARENT_ACL_NOT_FOUND() {
        return "由于未知对象，当前无法赋权。请稍后再试，或联系技术支持。";
    }

    @Override
    public String getIDENTITY_EXIST_CHILDREN() {
        return "'%s' 存在下级授权";
    }

    // Model
    @Override
    public String getINVALID_MODEL_DEFINITION() {
        return "无法找到模型。请检查后重试。";
    }

    @Override
    public String getEMPTY_MODEL_NAME() {
        return "模型名称不可为空";
    }

    @Override
    public String getINIT_MEASURE_FAILED() {
        return "无法初始化元数据。请尝试重新启动。若问题依然存在，请联系技术支持。";
    }

    @Override
    public String getINVALID_MODEL_NAME() {
        return "无效的模型名称 “%s”。请使用字母、数字或下划线命名。";
    }

    @Override
    public String getINVALID_DIMENSION_NAME() {
        return "无效的维度名称 “%s”。请使用中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getINVALID_MEASURE_NAME() {
        return "无效的度量名称 “%s”。请使用中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getDUPLICATE_MODEL_NAME() {
        return "模型名称 '%s' 已存在, 不能被创建";
    }

    @Override
    public String getBROKEN_MODEL_OPERATION_DENIED() {
        return "无法操作 BROKEN 模型 “%s”。";
    }

    @Override
    public String getMODEL_NOT_FOUND() {
        return "无法找到模型 “%s”。 请检查后重试。";
    }

    @Override
    public String getMODEL_MODIFY_ABANDON(String table) {
        return String.format(Locale.ROOT, "模型不支持被修改，因为没有 ['%s'] 权限", table);
    }

    @Override
    public String getEMPTY_PROJECT_NAME() {
        return "没有项目信息，请指定一个项目。";
    }

    @Override
    public String getGRANT_TABLE_WITH_SID_HAS_NOT_PROJECT_PERMISSION() {
        return "添加表级权限失败。用户（组） [%s] 无项目 [%s] 权限。请先授予用户（组）项目级权限。";
    }

    @Override
    public String getPROJECT_UNMODIFIABLE_REASON() {
        return "当前项目暂不支持模型推荐及优化。请在设置中启用智能推荐后重试。";
    }

    @Override
    public String getPROJECT_ONGOING_OPTIMIZATION() {
        return "当前有进行中的优化任务。请稍后再试。";
    }

    @Override
    public String getDUPLICATE_DIMENSION_NAME() {
        return "维度名称 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getDUPLICATE_MEASURE_NAME() {
        return "度量名称 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "该度量的定义和度量 “%s” 相同。请修改。";
    }

    public String getDUPLICATE_INTERNAL_MEASURE_DEFINITION() {
        return "该度量的定义和隐藏度量 “%s” 相同。请修改。";
    }

    @Override
    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "“%s” 和 “%s” 已存在联接条件，不能被创建。请修改。";
    }

    @Override
    public String getCheckCCType() {
        return "可计算列 “{0}” 定义的数据类型 “{2}” 与实际类型 “{1}” 不符。请修改。";
    }

    @Override
    public String getCheckCCExpression() {
        return "无法校验表达式 “%s” (可计算列：%s)。请检查表达式的正确性，或稍后重试。";
    }

    @Override
    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "无法解析文件。请检查该文件的完整性。";
    }

    @Override
    public String getEXPORT_BROKEN_MODEL() {
        return "无法导出模型 “%s”，因为该模型状态为 “BROKEN” 。请重新选择并重试。";
    }

    @Override
    public String getIMPORT_BROKEN_MODEL() {
        return "无法导入 Broken 的模型 [%s]。";
    }

    @Override
    public String getIMPORT_MODEL_EXCEPTION() {
        return "无法导入模型。";
    }

    @Override
    public String getUN_SUITABLE_IMPORT_TYPE(String optionalType) {
        if (optionalType == null) {
            return "导入类型 “%s“ 对模型 “%s” 不可用，仅可选择 “UN_IMPORT”。";
        } else {
            return "导入类型 “%s“ 对模型 “%s” 不可用，仅可选择 “UN_IMPORT” (或 “" + optionalType + "”)。";
        }
    }

    @Override
    public String getCAN_NOT_OVERWRITE_MODEL() {
        return "无法覆盖模型 “%s“，因为该模型不存在。请重新选择后重试。";
    }

    @Override
    public String getILLEGAL_MODEL_METADATA_FILE() {
        return "无法解析元数据文件。请勿修改或重新压缩导出的文件。";
    }

    @Override
    public String getEXPORT_AT_LEAST_ONE_MODEL() {
        return "请至少选择一个模型进行导出。";
    }

    @Override
    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED() {
        return "该可计算列的表达式已被用于模型 '%s'，名称为 '%s'。请修改名称以保持一致，或使用其他的表达式。";
    }

    @Override
    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED_SINGLE_MODEL() {
        return "该可计算列表达式在模型内已存在。请修改。";
    }

    @Override
    public String getCOMPUTED_COLUMN_NAME_DUPLICATED() {
        return "可计算列名称 '%s' 已被用于模型 '%s'，表达式为 '%s'。请修改表达式以保持一致，或使用其他的名称。";
    }

    @Override
    public String getCOMPUTED_COLUMN_NAME_DUPLICATED_SINGLE_MODEL() {
        return "该可计算列名在模型内已存在。请修改。";
    }

    @Override
    public String getMODEL_CHANGE_PERMISSION() {
        return "没有权限操作。仅系统管理员和项目管理员可以更改模型的所有者。";
    }

    @Override
    public String getMODEL_OWNER_CHANGE_INVALID_USER() {
        return "该用户无法被设置成模型所有者。请选择系统管理员、项目管理员、或模型管理员。";
    }

    // index
    @Override
    public String getINDEX_STATUS_TYPE_ERROR() {
        return "参数 “status” 仅支持 “NO_BUILD”, “ONLINE”, “LOCKED”, “BUILDING”";
    }

    @Override
    public String getINDEX_SOURCE_TYPE_ERROR() {
        return "参数 “sources” 仅支持 “RECOMMENDED_AGG_INDEX”, “RECOMMENDED_TABLE_INDEX”, “CUSTOM_AGG_INDEX”, “CUSTOM_TABLE_INDEX”.";
    }

    @Override
    public String getINDEX_SORT_BY_ERROR() {
        return "参数 “sort_by” 仅支持 “last_modified”, “usage”, “data_size”.";
    }

    // Job
    @Override
    public String getILLEGAL_TIME_FILTER() {
        return "选择的时间范围无效。请重新选择";
    }

    @Override
    public String getILLEGAL_EXECUTABLE_STATE() {
        return "选择的任务状态无效。请重新选择";
    }

    @Override
    public String getILLEGAL_STATE_TRANSFER() {
        return "任务状态更新时遇到错误，无法执行当前操作。请刷新任务列表后重试。";
    }

    @Override
    public String getINVALID_PRIORITY() {
        return "选择的优先级无效。请选择在 0 到 4 范围内的数值。";
    }

    // Acl
    @Override
    public String getUSER_NOT_EXIST() {
        return "用户 '%s' 不存在, 请确认用户是否存在。";
    }

    @Override
    public String getUSERGROUP_EXIST() {
        return "用户组 “%s” 已存在。请检查后重试。";
    }

    // Project
    @Override
    public String getINVALID_PROJECT_NAME() {
        return "项目名称只支持数字、字母和下划线，并且需要用数字或者字母开头。";
    }

    @Override
    public String getPROJECT_NAME_IS_ILLEGAL() {
        return "项目名称不得超过50个字符。请重新输入。";
    }

    @Override
    public String getPROJECT_ALREADY_EXIST() {
        return "项目名 \"%s\" 已存在。请重新命名。";
    }

    @Override
    public String getPROJECT_NOT_FOUND() {
        return "无法找到项目 \"%s\"，请检查后重试。";
    }

    @Override
    public String getPROJECT_DROP_FAILED_SECOND_STORAGE_ENABLED() {
        return "无法删除项目 \"%s\"，请先关闭分层存储。";
    }

    @Override
    public String getPROJECT_DROP_FAILED_JOBS_NOT_KILLED() {
        return "无法删除项目 \"%s\"，请终止相关任务后再试。";
    }

    @Override
    public String getSQL_LIST_IS_EMPTY() {
        return "请输入参数 “sqls“。";
    }

    @Override
    public String getPROJECT_CHANGE_PERMISSION() {
        return "没有权限操作。仅系统管理员可以更改项目的所有者。";
    }

    @Override
    public String getPROJECT_OWNER_CHANGE_INVALID_USER() {
        return "该用户无法被设置成项目所有者。请选择系统管理员，或该项目的管理员。";
    }

    @Override
    public String getPROJECT_DISABLE_MLP() {
        return "此项目未开启多级分区，多级分区无法使用。请开启后重试。";
    }

    // Table
    public String getHIVE_TABLE_NOT_FOUND() {
        return "找不到 Hive 表 '%s'";
    }

    // table sampling
    @Override
    public String getBEYOND_MIX_SAMPLING_ROWSHINT() {
        return "采样的行数应大于 %d。 请修改。";
    }

    @Override
    public String getBEYOND_MAX_SAMPLING_ROWS_HINT() {
        return "采样的行数应小于 %d。请修改。";
    }

    @Override
    public String getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME() {
        return "无效的采样表名称。请以 “database.table” 为格式命名。";
    }

    @Override
    public String getFAILED_FOR_NO_SAMPLING_TABLE() {
        return "无法执行表采样。请选择至少一张表。";
    }

    @Override
    public String getRELOAD_TABLE_CC_RETRY() {
        return "%s源表 %s 中列 %s 的数据类型发生了变更。请删除可计算列或修改数据类型后再试。";
    }

    @Override
    public String getRELOAD_TABLE_MODEL_RETRY() {
        return "源表 %1$s 中列 %2$s 的数据类型发生变更。请从模型 %3$s 中删除该列，或修改该列的数据类型。";
    }

    @Override
    public String getSAME_TABLE_NAME_EXIST() {
        return "表 %s 已经存在，请选择其他名称。";
    }

    @Override
    public String getQUERY_NOT_ALLOWED() {
        return "任务节点不支持查询。请选择查询节点。";
    }

    @Override
    public String getNOT_SUPPORTED_SQL() {
        return "该 SQL 暂不支持。请尝试不同的 SQL。";
    }

    @Override
    public String getDUPLICATE_QUERY_NAME() {
        return "名为 \"%s\" 的查询已存在。请检查后重试。";
    }

    @Override
    public String getNULL_EMPTY_SQL() {
        return "SQL 不能为空。请检查后重试。";
    }

    @Override
    public String getJOB_REPEATED_START_FAILURE() {
        return "不能重复启动流数据任务。";
    }

    @Override
    public String getJOB_START_FAILURE() {
        return "模型 \"%s\" 当前已有运行中的流数据任务，无法重复启动。请检查后重试。";
    }

    @Override
    public String getJOB_BROKEN_MODEL_START_FAILURE() {
        return "无法启动，模型 \"%s\" 当前为 Broken 状态。";
    }

    @Override
    public String getJOB_STOP_FAILURE() {
        return "当前无法停止模型 \"%s\" 的流数据任务。请查看日志，或稍后重试。";
    }

    // Access
    @Override
    public String getACL_PERMISSION_REQUIRED() {
        return "需要授权";
    }

    @Override
    public String getSID_REQUIRED() {
        return "找不到 Sid";
    }

    @Override
    public String getEMPTY_PERMISSION() {
        return "权限不能为空";
    }

    @Override
    public String getINVALID_PERMISSION() {
        return "参数 \"permission\" 的值无效，请使用 \"ADMIN\"、\"MANAGEMENT\"、\"OPERATION\" 或 \"QUERY\"";
    }

    @Override
    public String getINVALID_PARAMETER_TYPE() {
        return "参数 \"type\" 的值无效，请使用 \"user\" 或 \"group\"";
    }

    @Override
    public String getUNAUTHORIZED_SID() {
        return "用户/组没有当前项目访问权限";
    }

    @Override
    public String getACCESS_DENY() {
        return "当前用户无访问权限。";
    }

    // user group

    @Override
    public String getEMPTY_GROUP_NAME() {
        return "用户组名不能为空.";
    }

    @Override
    public String getEMPTY_SID() {
        return "用户名/用户组名不能为空";
    }

    @Override
    public String getEMPTY_QUERY_NAME() {
        return "查询名称不能为空";
    }

    @Override
    public String getINVALID_QUERY_NAME() {
        return "查询名称只能包含字母，数字和下划线";
    }

    @Override
    public String getREVOKE_ADMIN_PERMISSION() {
        return "不能取消创建者的管理员权限";
    }

    @Override
    public String getGroup_EDIT_NOT_ALLOWED() {
        return "暂不支持LDAP认证机制下的用户组编辑操作";
    }

    @Override
    public String getGroup_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "暂不支持客户认证接入机制下的用户组编辑操作, 方法 '%s' 未被实现";
    }

    // Async Query
    @Override
    public String getQUERY_RESULT_NOT_FOUND() {
        return "该项目下无法找到该 Query ID 对应的异步查询。请检查后重试。";
    }

    @Override
    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "无法找到查询结果文件。请检查后重试。";
    }

    @Override
    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "无法获取失败异步查询的查询状态。请检查后重试。";
    }

    @Override
    public String getCLEAN_FOLDER_FAIL() {
        return "当前无法清理文件夹。请确保相关 HDFS 文件可以正常访问。";
    }

    @Override
    public String getASYNC_QUERY_TIME_FORMAT_ERROR() {
        return "无效的时间格式。请按 “yyyy-MM-dd HH:mm:ss” 格式填写。";
    }

    public String getASYNC_QUERY_PROJECT_NAME_EMPTY() {
        return "项目名称不能为空。请检查后重试。";
    }

    // User
    @Override
    public String getAUTH_INFO_NOT_FOUND() {
        return "找不到权限信息";
    }

    @Override
    public String getUSER_NOT_FOUND() {
        return "找不到用户 '%s'";
    }

    @Override
    public String getUSER_BE_LOCKED(long seconds) {
        return "用户名或密码错误，请在 " + formatSeconds(seconds) + "后再次重试。";
    }

    @Override
    public String getUSER_IN_LOCKED_STATUS(long leftSeconds, long nextLockSeconds) {
        return "为了账号安全，用户 %s 被锁定。请在 " + formatSeconds(leftSeconds) + "后再试。" + formatNextLockDuration(nextLockSeconds);
    }

    @Override
    protected String formatNextLockDuration(long nextLockSeconds) {
        if (Long.MAX_VALUE == nextLockSeconds) {
            return "如登录再次错误，将被永久锁定。";
        }
        return "如登录再次错误将会被继续锁定 " + formatSeconds(nextLockSeconds) + "。";
    }

    @Override
    protected String formatTime(long day, long hour, long min, long second) {
        StringBuilder stringBuilder = new StringBuilder();
        if (day > 0) {
            stringBuilder.append(day).append(" 天 ");
        }
        if (hour > 0) {
            stringBuilder.append(hour).append(" 小时 ");
        }
        if (min > 0) {
            stringBuilder.append(min).append(" 分 ");
        }
        if (second > 0) {
            stringBuilder.append(second).append(" 秒 ");
        }
        return stringBuilder.toString();
    }

    @Override
    public String getUSER_IN_PERMANENTLY_LOCKED_STATUS() {
        return "用户 %s 已被永久锁定，请联系您的系统管理员进行重置。";
    }

    @Override
    public String getUSER_AUTH_FAILED() {
        return "用户名或密码错误。请检查后重试。";
    }

    @Override
    public String getNEW_PASSWORD_SAME_AS_OLD() {
        return "新密码与旧密码一致，请输入一个不同的新密码";
    }

    @Override
    public String getUSER_EDIT_NOT_ALLOWED() {
        return "暂不支持LDAP认证机制下的用户编辑操作";
    }

    @Override
    public String getUSER_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "暂不支持客户认证接入机制下的用户编辑操作, 方法 '%s' 未被实现";
    }

    @Override
    public String getOWNER_CHANGE_ERROR() {
        return "更改失败，请重试。";
    }

    @Override
    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "诊断包不可用, 路径: %s";
    }

    @Override
    public String getDIAG_FAILED() {
        return "生成诊断包失败。请尝试重新生成。";
    }

    // Basic
    @Override
    public String getFREQUENCY_THRESHOLD_CAN_NOT_EMPTY() {
        return "查询频率阈值不能为空";
    }

    @Override
    public String getRECOMMENDATION_LIMIT_NOT_EMPTY() {
        return "新增索引上限不能为空";
    }

    @Override
    public String getDELAY_THRESHOLD_CAN_NOT_EMPTY() {
        return "查询延迟阈值不能为空";
    }

    @Override
    public String getMIN_HIT_COUNT_NOT_EMPTY() {
        return "命中次数不能为空";
    }

    @Override
    public String getEFFECTIVE_DAYS_NOT_EMPTY() {
        return "时间范围不能为空";
    }

    @Override
    public String getUPDATE_FREQUENCY_NOT_EMPTY() {
        return "推荐频率不能为空";
    }

    @Override
    public String getSQL_NUMBER_EXCEEDS_LIMIT() {
        return "最多可同时导入 %s 条 SQL";
    }

    @Override
    public String getSQL_FILE_TYPE_MISMATCH() {
        return "sql文件的后缀必须是 'txt' 或 'sql'";
    }

    @Override
    public String getConfigNotSupportDelete() {
        return "无法删除该配置。";
    }

    @Override
    public String getConfigNotSupportEdit() {
        return "无法编辑该配置。";
    }

    @Override
    public String getConfigMapEmpty() {
        return "配置列表不能为空。请检查后重试。";
    }

    // Query statistics

    @Override
    public String getNOT_SET_INFLUXDB() {
        return "未设置参数 kap.metric.write-destination 为 INFLUX";
    }

    //license
    @Override
    public String getLICENSE_ERROR_PRE() {
        return "无法更新许可证：\n";
    }

    @Override
    public String getLICENSE_ERROR_SUFF() {
        return "\n请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_OVERDUE_TRIAL() {
        return "许可证已过期，当前有效期为[%s - %s]。请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_NODES_EXCEED() {
        return "您使用的节点数已超过许可证范围，请联系您的客户经理。";
    }

    @Override
    public String getLICENSE_NODES_NOT_MATCH() {
        return "当前许可证的节点数与集群信息不匹配，请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_WRONG_CATEGORY() {
        return "当前许可证的版本与产品不匹配，请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_NO_LICENSE() {
        return "没有许可证文件。请联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_INVALID_LICENSE() {
        return "无效许可证。请上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_MISMATCH_LICENSE() {
        return "该许可证适用的集群信息与当前不符。请上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_NOT_EFFECTIVE() {
        return "许可证尚未生效，请重新申请。";
    }

    @Override
    public String getLICENSE_EXPIRED() {
        return "该许可证已过期。请上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getLICENSE_SOURCE_OVER_CAPACITY() {
        return "当前已使用数据量（%s/%s）超过许可证上限。系统无法进行构建或数据加载任务。\n" + "请联系 Kyligence 销售人员，或尝试删除一些 Segment 以解除限制。";
    }

    @Override
    public String getLICENSE_PROJECT_SOURCE_OVER_CAPACITY() {
        return "当前项目已使用数据量（%s/%s）超过配置上限。系统无法进行构建或数据加载任务。\n" + "请联系 Kyligence 销售人员，或尝试删除一些 Segment 以解除限制。";
    }

    @Override
    public String getLICENSE_NODES_OVER_CAPACITY() {
        return "当前已使用节点数（%s/%s）超过许可证上限。系统无法进行构建或数据加载任务。\n" + "请联系 Kyligence 销售人员，或尝试停止部分节点以解除限制。";
    }

    @Override
    public String getLICENSE_SOURCE_NODES_OVER_CAPACITY() {
        return "当前已使用数据量（%s/%s）和节点数（%s/%s）均超过许可证上限。\n" + "系统无法进行构建或数据加载任务。\n"
                + "请联系 Kyligence 销售人员，或尝试删除一些 segments 并停止部分节点以解除限制。";
    }

    @Override
    public String getLICENSE_PROJECT_SOURCE_NODES_OVER_CAPACITY() {
        return "当前项目已使用数据量（%s/%s）和节点数（%s/%s）均超过配置上限。\n" + "系统无法进行构建或数据加载任务。\n"
                + "请联系 Kyligence 销售人员，或尝试删除一些 segments 并停止部分节点以解除限制。";
    }

    @Override
    public String getTABLENOTFOUND() {
        return "模型 “%s” 保存失败。请确保模型中使用的列 “%s” 在源表 “%s” 中存在。";
    }

    // Async push down get date format
    @Override
    public String getPUSHDOWN_PARTITIONFORMAT_ERROR() {
        return "自动探测失败，请手动选择分区格式。";
    }

    // Async push down get data range
    @Override
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "自动探测失败，请手动选择数据范围。";
    }

    @Override
    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "自动探测失败，请手动选择数据范围。";
    }

    @Override
    public String getDIMENSION_NOTFOUND() {
        return "维度 %s 正在被索引、聚合组引用。请到”数据资产-模型-索引”查看，删除引用的聚合组、索引。";
    }

    @Override
    public String getMEASURE_NOTFOUND() {
        return "度量 %s 正在被索引、聚合组引用。请到”数据资产-模型-索引”查看，删除引用的聚合组、索引。";
    }

    @Override
    public String getNESTED_CC_CASCADE_ERROR() {
        return "无法修改可计算列 “%s”。当前模型中存在嵌套可计算列 “%s” 依赖于当前可计算列。请先解除引用关系后再进行修改。";
    }

    public String getCC_ON_ANTI_FLATTEN_LOOKUP() {
        return "无法在可计算列中使用维度表 “%s” 中的列，因为该表的关联关系不进行预计算。";
    }

    public String getFILTER_CONDITION_ON_ANTI_FLATTEN_LOOKUP() {
        return "无法在数据筛选条件中使用维度表 “%s” 中的列，因为该表的关联关系不进行预计算。";
    }

    @Override
    public String getCHANGE_GLOBALADMIN() {
        return "您不可以添加，修改，删除系统管理员的权限。";
    }

    @Override
    public String getCHANGE_DEGAULTADMIN() {
        return "无法修改系统默认内置管理员 ADMIN 用户的权限。";
    }

    //Query
    @Override
    public String getINVALID_USER_TAG() {
        return "无法添加标签，因为长度超出了256 个字符。请修改。";
    }

    @Override
    public String getINVALID_ID() {
        return "无法找到 ID \"%s\"。请检查后重试。";
    }

    @Override
    public String getSEGMENT_LOCKED() {
        return "Segment “%s” 被锁定，无法删除、刷新或合并。请稍后重试。";
    }

    @Override
    public String getSEGMENT_STATUS(String status) {
        return "Segment “%s” 处于 “" + status + "” 状态，无法刷新或合并。请稍后重试。";
    }

    //Kerberos
    @Override
    public String getPRINCIPAL_EMPTY() {
        return "Principal 名称不能为空.";
    }

    @Override
    public String getKEYTAB_FILE_TYPE_MISMATCH() {
        return "keytab 文件后缀必须是 'keytab'";
    }

    @Override
    public String getKERBEROS_INFO_ERROR() {
        return "无效的 Principal 名称或者 Keytab 文件，请检查后重试.";
    }

    @Override
    public String getPROJECT_HIVE_PERMISSION_ERROR() {
        return "权限不足，请确保提交的 Kerberos 用户信息包含所有已加载表的访问权限.";
    }

    //HA
    @Override
    public String getNO_ACTIVE_LEADERS() {
        return "系统中暂无活跃的任务节点。请联系系统管理员进行检查并修复。";
    }

    @Override
    public String getLEADERS_HANDLE_OVER() {
        return "系统正在尝试恢复服务。请稍后重试。";
    }

    @Override
    public String getTABLE_REFRESH_NOTFOUND() {
        return "连接数据源异常。请尝试重新刷新。";
    }

    @Override
    public String getTABLE_REFRESH_ERROR() {
        return "连接数据源异常。请尝试重新刷新。";
    }

    @Override
    public String getTABLE_REFRESH_PARAM_INVALID() {
        return "请求中的 “tables” 字段无效。请检查后重试。";
    }

    @Override
    public String getTABLE_REFRESH_PARAM_MORE() {
        return "请求中包含非 “tables“ 的多余字段。请检查后重试。";
    }

    @Override
    public String getTRANSFER_FAILED() {
        return "请求转发失败。请稍后重试。";
    }

    @Override
    public String getUSER_EXISTS() {
        return "用户名:[%s] 已存在。";
    }

    @Override
    public String getOPERATION_FAILED_BY_USER_NOT_EXIST() {
        return "操作失败，用户[%s]不存在，请先添加";
    }

    @Override
    public String getOPERATION_FAILED_BY_GROUP_NOT_EXIST() {
        return "操作失败，用户组[%s]不存在，请先添加";
    }

    @Override
    public String getPERMISSION_DENIED() {
        return "拒绝访问";
    }

    @Override
    public String getCOLUMU_IS_NOT_DIMENSION() {
        return "请先添加列 “%s” 为维度。";
    }

    @Override
    public String getMODEL_CAN_NOT_PURGE() {
        return "当前项目设置下，不支持指定模型 “%s” 清除数据。";
    }

    @Override
    public String getMODEL_SEGMENT_CAN_NOT_REMOVE() {
        return "当前项目设置下，无法手动删除模型 “%s” 中的 Segment。";
    }

    @Override
    public String getSEGMENT_CAN_NOT_REFRESH() {
        return "有部分 Segment 正在构建，无法刷新。请稍后重试。";
    }

    @Override
    public String getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE() {
        return "当前无法刷新 Segment，因为范围已更改。请稍后重试。";
    }

    @Override
    public String getCAN_NOT_BUILD_SEGMENT() {
        return "无法构建 Segment。请先添加索引。";
    }

    @Override
    public String getCAN_NOT_BUILD_SEGMENT_MANUALLY() {
        return "当前项目设置下，无法手动构建模型 “%s” 的 Segment。";
    }

    @Override
    public String getCAN_NOT_BUILD_INDICES_MANUALLY() {
        return "当前项目设置下，无法手动构建模型 “%s” 的索引。";
    }

    @Override
    public String getINVALID_MERGE_SEGMENT() {
        return "无法合并暂不可用的 Segment。";
    }

    @Override
    public String getINVALID_SET_TABLE_INC_LOADING() {
        return "无法设置表 “％s” 的增量加载，因为其已在模型 “％s” 中作为维表使用。";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT() {
        return "当前没有可用的 Segment 可以刷新，请稍后重试。";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT_BY_NOT_READY() {
        return "当前无法刷新，请确保刷新范围内的所有 Segment 均已就绪。";
    }

    @Override
    public String getINVALID_LOAD_HIVE_TABLE_NAME() {
        return "无法执行该操作。请设置 ”kap.table.load-hive-tablename-cached.enabled=true”，然后重试。";
    }

    @Override
    public String getINVALID_REMOVE_USER_FROM_ALL_USER() {
        return "无法从ALL USERS组中删除用户。";
    }

    @Override
    public String getACCESS_DENY_ONLY_ADMIN() {
        return "拒绝访问，只有系统和项目管理员才能编辑用户的表，列和行权限";
    }

    @Override
    public String getACCESS_DENY_ONLY_ADMIN_AND_PROJECT_ADMIN() {
        return "拒绝访问，只有系统管理员才能编辑用户的表，列和行权限";
    }

    @Override
    public String getQUERY_TOO_MANY_RUNNING() {
        return "查询请求数量超过上限，无法提交。请稍后再试，或联系项目管理员修改设置。";
    }

    @Override
    public String getSELF_DISABLE_FORBIDDEN() {
        return "您不可以禁用您自己";
    }

    @Override
    public String getSELF_DELETE_FORBIDDEN() {
        return "您不可以删除您自己";
    }

    @Override
    public String getSELF_EDIT_FORBIDDEN() {
        return "无效的操作对象，请检查后重试。";
    }

    @Override
    public String getOLD_PASSWORD_WRONG() {
        return "原密码不正确";
    }

    @Override
    public String getINVALID_PASSWORD() {
        return "密码应至少包含一个数字，字母和特殊字符（〜！@＃$％^＆*（）{} |：\\“ <>？[]; \\'\\，。/`）。";
    }

    @Override
    public String getSHORT_PASSWORD() {
        return "密码应包含8个以上的字符！";
    }

    @Override
    public String getSEGMENT_LIST_IS_EMPTY() {
        return "找不到 Segment。请检查后重试。";
    }

    @Override
    public String getSEGMENT_ID_NOT_EXIST() {
        return "找不到 ID 为 “%s” 的 Segment。请检查后重试。";
    }

    @Override
    public String getSEGMENT_NAME_NOT_EXIST() {
        return "找不到名为 “%s” 的 Segment。请检查后重试。”。";
    }

    @Override
    public String getLAYOUT_LIST_IS_EMPTY() {
        return "找不到 Layout。请检查后重试。";
    }

    @Override
    public String getLAYOUT_NOT_EXISTS() {
        return "找不到 Layout “%s”。请检查后重试。";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT() {
        return "请至少选一个 Segment 刷新。";
    }

    @Override
    public String getEMPTY_SEGMENT_PARAMETER() {
        return "请输入 Segment ID 或名称。";
    }

    @Override
    public String getCONFLICT_SEGMENT_PARAMETER() {
        return "不能同时输入 Segment ID 和名称。请重新输入。";
    }

    @Override
    public String getINVALID_MERGE_SEGMENT_BY_TOO_LESS() {
        return "请至少选择两个 Segment 合并。";
    }

    @Override
    public String getCONTENT_IS_EMPTY() {
        return "许可证内容为空";
    }

    @Override
    public String getILLEGAL_EMAIL() {
        return "不允许使用个人电子邮件或非法电子邮件";
    }

    @Override
    public String getLICENSE_ERROR() {
        return "获取许可证失败";
    }

    @Override
    public String getEMAIL_USERNAME_COMPANY_CAN_NOT_EMPTY() {
        return "邮箱, 用户名, 公司不能为空";
    }

    @Override
    public String getEMAIL_USERNAME_COMPANY_IS_ILLEGAL() {
        return "邮箱, 用户名, 公司的长度要小于等于50";
    }

    @Override
    public String getUSERNAME_COMPANY_IS_ILLEGAL() {
        return "用户名, 公司只支持中英文、数字、空格";
    }

    @Override
    public String getINVALID_COMPUTER_COLUMN_NAME_WITH_KEYWORD() {
        return "可计算列 \"%s\" 的名称是 SQL 关键字。请使用其他名称。";
    }

    @Override
    public String getINVALID_COMPUTER_COLUMN_NAME() {
        return "无效的计算列名称 “%s”。请以字母开头，并只使用字母、数字、下划线。请重新命名。";
    }

    @Override
    public String getMODEL_ALIAS_DUPLICATED() {
        return "模型 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getINVALID_RANGE_LESS_THAN_ZERO() {
        return "起始时间和终止时间必须大于 0。请修改。";
    }

    @Override
    public String getINVALID_RANGE_NOT_FORMAT() {
        return "起始或终止时间格式无效。仅支持时间戳，单位毫秒（ms）。请修改。";
    }

    @Override
    public String getINVALID_RANGE_END_LESSTHAN_START() {
        return "终止时间必须大于起始时间。请修改。";
    }

    @Override
    public String getINVALID_RANGE_NOT_CONSISTENT() {
        return "起始时间和终止时间必须同时存在或者同时不存在。请修改。";
    }

    @Override
    public String getID_CANNOT_EMPTY() {
        return "ID 不能为空。请检查后重试。";
    }

    @Override
    public String getINVALID_CREATE_MODEL() {
        return "无法在此项目中手动添加模型。";
    }

    @Override
    public String getSEGMENT_INVALID_RANGE() {
        return "无法刷新，Segment 范围 “%s” 超出了加载数据的范围 “%s”。请修改后重试。";
    }

    @Override
    public String getSEGMENT_RANGE_OVERLAP() {
        return "无法构建，待构建的范围和已构建的范围在 “%s” 到 “%s” 之间存在重合。请修改后重试。";
    }

    @Override
    public String getPARTITION_COLUMN_NOT_EXIST() {
        return "无法找到分区列。请检查后重试。";
    }

    @Override
    public String getPARTITION_COLUMN_START_ERROR() {
        return "无法启动。请确保模型的时间分区列为时间戳类型，且时间格式有效。";
    }

    @Override
    public String getPARTITION_COLUMN_SAVE_ERROR() {
        return "无法提交。请确保模型的时间分区列为时间戳类型，且时间格式有效。";
    }

    @Override
    public String getTIMESTAMP_COLUMN_NOT_EXIST() {
        return "无法加载。请确保表中有 timestamp 类型的列。";
    }

    @Override
    public String getTIMESTAMP_PARTITION_COLUMN_NOT_EXIST() {
        return "无法保存模型。融合模型必须将时间分区列加入模型维度。";
    }

    @Override
    public String getINVALID_PARTITION_COLUMN() {
        return "请选择事实表上的原始列（而非可计算列）作为时间分区列。";
    }

    @Override
    public String getTABLE_NAME_CANNOT_EMPTY() {
        return "表名不能为空。请检查后重试。";
    }

    @Override
    public String getTABLE_SAMPLE_MAX_ROWS() {
        return "表抽样取值范围应在 10,000 至 20,000,000 行之间。";
    }

    @Override
    public String getTABLE_NOT_FOUND() {
        return "无法找到表 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getTABLE_PARAM_EMPTY() {
        return "无法找到该表，请检查后重试。";
    }

    @Override
    public String getILLEGAL_JOB_STATE() {
        return "选择的任务状态无效，状态必须是 “PENDING“, “RUNNING“, “FINISHED“, “ERROR” 或 “DISCARDED“。请检查后重试。";
    }

    @Override
    public String getILLEGAL_JOB_ACTION() {
        return "无效的参数值 “action“ 或 “statuses“ 或 “job_ids“。"
                + "“statuses“值或“job_ids“指定任务的状态 值包含 “%s“，此状态的任务只能执行以下操作 “%s“";
    }

    @Override
    public String getFILE_NOT_EXIST() {
        return "找不到文件[%s]";
    }

    @Override
    public String getDATABASE_NOT_EXIST() {
        return "无法找到数据库 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getBROKEN_MODEL_CANNOT_ONOFFLINE() {
        return "模型 “%s” 无法上线或下线，因为其处于 BROKEN 状态。";
    }

    @Override
    public String getINVALID_NAME_START_WITH_DOT() {
        return "用户名/用户组名不能以英文句号开头(.)";
    }

    @Override
    public String getINVALID_NAME_START_OR_END_WITH_BLANK() {
        return "用户名/用户组名不能以空格开头或结尾";
    }

    @Override
    public String getINVALID_NAME_LEGTHN() {
        return "用户名需要小于180字符，请检查后重试。";
    }

    @Override
    public String getINVALID_NAME_CONTAINS_OTHER_CHARACTER() {
        return "用户名/用户组中仅支持英文字符";
    }

    @Override
    public String getINVALID_NAME_CONTAINS_INLEGAL_CHARACTER() {
        return "用户名/用户组名中不能包含如下符号: 反斜杠(\\), 斜杠(/), 冒号(:), 星号(*), 问号(?), 引号(“), 小于号(<), 大于号(>), 垂直线(|)";
    }

    @Override
    public String getHIVETABLE_NOT_FOUND() {
        return "无法加载表 \"%s\"。请确保以上表在数据源中存在。";
    }

    @Override
    public String getDUPLICATE_LAYOUT() {
        return "无法添加该索引，因为已存在相同的索引。请修改。";
    }

    @Override
    public String getDEFAULT_REASON() {
        return "遇到了一些问题。%s";
    }

    @Override
    public String getDEFAULT_SUGGEST() {
        return "更多详情请联系 Kyligence 技术支持。";
    }

    @Override
    public String getUNEXPECTED_TOKEN() {
        return "以下内容存在语法错误（%s 列，%s 行）：\"%s\" 。请修改。";
    }

    @Override
    public String getBAD_SQL_REASON() {
        return "SQL 存在语法错误：%s";
    }

    @Override
    public String getBAD_SQL_SUGGEST() {
        return "请修改。";
    }

    @Override
    public String getBAD_SQL_TABLE_NOT_FOUND_REASON() {
        return "无法找到表 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getBAD_SQL_TABLE_NOT_FOUND_SUGGEST() {
        return "请在数据源中导入表 \"%s\"。如果该表已经存在，请在查询中以\"数据库名.表名\"的形式进行引用。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_REASON() {
        return "无法找到列 \"%s\"。请检查此列是否在源表中存在。若存在，可尝试重载表；若不存在，请联系管理员添加。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST() {
        return "无法找到列 \"%s\"。请检查此列是否在源表中存在。若存在，可尝试重载表；若不存在，请联系管理员添加。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON() {
        return "无法找到列 \"%s\"。请检查此列是否在源表中存在。若存在，可尝试重载表；若不存在，请联系管理员添加。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGESTION() {
        return "无法找到列 \"%s\"。请检查此列是否在源表中存在。若存在，可尝试重载表；若不存在，请联系管理员添加。";
    }

    @Override
    public String getPROJECT_NUM_OVER_THRESHOLD() {
        return "新建项目失败，项目数超过最大值：{%s}，请删除其他废弃项目后再尝试新建或联系管理员调整最大项目数。";
    }

    @Override
    public String getMODEL_NUM_OVER_THRESHOLD() {
        return "新建模型失败。模型超过最大值：{%s}。请删除其他废弃模型后再尝试新建或联系管理员调整最大模型数。";
    }

    @Override
    public String getQUERY_ROW_NUM_OVER_THRESHOLD() {
        return "查询失败，查询结果行数超过最大值 \"%s\"。请添加过滤条件或联系管理员调整最大查询结果行数。";
    }

    @Override
    public String getCC_EXPRESSION_CONFLICT(String newCCExpression, String newCCName, String existedCCName) {
        return String.format(Locale.ROOT, "可计算列 \"%s\" 的表达式 \"%s\" 与可计算列 \"%s\" 相同。请修改。", newCCName, newCCExpression,
                existedCCName);
    }

    @Override
    public String getCC_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "可计算列 \"%s\" 已存在。请修改。", ccName);
    }

    @Override
    public String get_ALIAS_CONFLICT_OF_APPROVING_RECOMMENDATION() {
        return "该名称已存在，请重新命名。";
    }

    @Override
    public String getDIMENSION_CONFLICT(String dimensionName) {
        return String.format(Locale.ROOT, "维度 \"%s\" 已存在。请修改。", dimensionName);
    }

    @Override
    public String getMEASURE_CONFLICT(String measureName) {
        return String.format(Locale.ROOT, "度量 \"%s\" 已存在。请修改。", measureName);
    }

    @Override
    public String getINVALID_TIME_FORMAT() {
        return "无法设置时间分区列，选取的时间分区列不符合时间格式。请重新选择。";
    }

    @Override
    public String getINVALID_CUSTOMIZE_FORMAT() {
        return "格式不支持，请检查后重新输入。";
    }

    @Override
    public String getSEGMENT_CONTAINS_GAPS() {
        return "无法合并所选 Segment，因为时间范围不连续。请检查后重试。";
    }

    @Override
    public String getSegmentMergeLayoutConflictError() {
        return "当前 Segments 所包含的索引不一致，请先构建索引并确保其一致后再合并。";
    }

    @Override
    public String getSegmentMergePartitionConflictError() {
        return "当前 Segments 所包含的分区不一致，请先构建分区并确保其一致后再合并。";
    }

    @Override
    public String getSegmentMergeStorageCheckError() {
        return "合并 Segment 过程中 HDFS 存储空间可能超过阈值限制，系统主动终止合并任务。如需解除上述限制，请参照用户手册对参数 kylin.cube.merge-segment-storage-threshold 进行调整。";
    }

    @Override
    public String getDIMENSION_TABLE_USED_IN_THIS_MODEL() {
        return "无法设置此模型的维度表，因为其已被用作当前模型的事实表。请修改后重试。";
    }

    @Override
    public String getNO_DATA_IN_TABLE() {
        return "无法从表 “%s” 中获取数据。请检查后重试。";
    }

    @Override
    public String getEFFECTIVE_DIMENSION_NOT_FIND() {
        return "以下列未作为维度添加到模型中，请删除后再保存或添加到模型中。\nColumn ID: %s";
    }

    @Override
    public String getINVALID_PASSWORD_ENCODER() {
        return "非法的PASSWORD ENCODER，请检查配置项kylin.security.user-password-encoder";
    }

    @Override
    public String getFAILED_INIT_PASSWORD_ENCODER() {
        return "PASSWORD ENCODER 初始化失败，请检查配置项kylin.security.user-password-encoder";

    }

    @Override
    public String getINVALID_INTEGER_FORMAT() {
        return "无法重写模型设置，“%s” 参数值必须为非负整数。请修改后重试。";
    }

    @Override
    public String getINVALID_MEMORY_SIZE() {
        return "无法重写模型设置，“spark-conf.spark.executor.memory” 参数值必须为非负整数，且单位为 GB。请修改后重试。";
    }

    @Override
    public String getINVALID_BOOLEAN_FORMAT() {
        return "无法重写模型设置，“%s” 参数值必须为 “true” 或 “false”。请修改后重试。";
    }

    @Override
    public String getINVALID_AUTO_MERGE_CONFIG() {
        return "无法重写模型设置，自动合并范围不能为空。请修改后重试。";
    }

    @Override
    public String getCOLUMN_NOT_EXIST() {
        return "列:[%s] 不存在.";
    }

    public String getCOLUMN_PARAMETER_INVALID(String column) {
        return String.format(Locale.ROOT, "无法给列 ”%s” 赋值，值和列的类型不匹配。请检查后重试。", column);
    }

    public String getINVALID_VOLATILE_RANGE_CONFIG() {
        return "无法重写模型设置，动态区间参数单位必须为“天”、“周”、“月”、“年”其中一个，且值必须为非负整数。请修改后重试。";
    }

    @Override
    public String getINVALID_RETENTION_RANGE_CONFIG() {
        return "重写模型设置失败，留存设置值必须为非负整数，单位必须为自动合并选中单位中的最粗粒度单位.";
    }

    @Override
    public String getINSUFFICIENT_AUTHENTICATION() {
        return "无法认证用户信息，请重新登录。";
    }

    @Override
    public String getDISABLED_USER() {
        return "该用户已被禁用，请联系管理员。";
    }

    @Override
    public String getJOB_NODE_INVALID(String url) {
        return "该请求无法在任务节点执行。请检查后重试。";
    }

    @Override
    public String getWRITE_IN_MAINTENANCE_MODE() {
        return "系统已进入维护模式，元数据相关操作暂不可用。请稍后再试。";
    }

    @Override
    public String getLICENSE_OVER_VOLUME() {
        return "当前系统已使用容量超过该许可证允许的容量。请上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getADD_JOB_CHECK_FAIL() {
        return "当前无法提交任务，因为已有相同对象的构建任务正在进行。请稍后再试。";
    }

    @Override
    public String getADD_JOB_CHECK_FAIL_WITHOUT_BASE_INDEX() {
        return "当前无法提交任务，Segment “%s” 不包含基础索引。请刷新此 Segment。";
    }

    @Override
    public String getADD_EXPORT_JOB_FAIL() {
        return "无法提交任务。模型当前已有相同 Segment 的加载数据任务正在进行。请稍后重试。";
    }

    @Override
    public String getADD_JOB_EXCEPTION() {
        return "当前没有可执行的任务。请稍后重试。";
    }

    @Override
    public String getADD_JOB_ABANDON() {
        return "无法添加任务，该节点不是构建节点。请检查后重试。";
    }

    @Override
    public String getSTORAGE_QUOTA_LIMIT() {
        return "已无可用的存储配额。系统提交构建任务失败，查询引擎依然正常服务。请及时清理低效存储，提高低效存储阈值，或者通知管理员提高本项目的存储配额。";
    }

    @Override
    public String getADD_JOB_CHECK_SEGMENT_FAIL() {
        return "无法添加任务，Segment 索引不一致。请检查后重试。";
    }

    @Override
    public String getEMPTY_DATABASE() {
        return "请输入参数 “Database” 的值。";
    }

    @Override
    public String getEMPTY_TABLE_LIST() {
        return "请输入参数 “Table” 的值。";
    }

    @Override
    public String getADD_JOB_CHECK_SEGMENT_READY_FAIL() {
        return "无法添加任务，当前没有 READY 状态的 Segment。请稍后重试。";
    }

    @Override
    public String getADD_JOB_CHECK_INDEX_FAIL() {
        return "无法添加任务，Segment 索引为空。请稍后重试。";
    }

    @Override
    public String getREFRESH_JOB_CHECK_INDEX_FAIL() {
        return "当前没有可刷新索引。请检查后重试。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_ABANDON() {
        return "无法添加任务。请确保该操作对当前对象有效。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_EMPTY() {
        return "无法添加任务，子分区值为空。请检查后重试。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_DUPLICATE() {
        return "无法添加任务。请确保不存在重复的子分区值。";
    }

    @Override
    public String getTABLE_RELOAD_ADD_COLUMN_EXIST(String table, String column) {
        return String.format(Locale.ROOT, "当前无法重载表。列 “%s” 在表 “%s” 中已存在。请修改后重试。", column, table);
    }

    @Override
    public String getTABLE_RELOAD_HAVING_NOT_FINAL_JOB() {
        return "当前暂不可重载表。存在运行中的任务，任务对象为： %s。请等任务完成后再重载，或手动终止任务。";
    }

    @Override
    public String getCOLUMN_UNRECOGNIZED() {
        return "无法识别表达式中的列名 “%s”。 请使用 “TABLE_ALIAS.COLUMN“ 来命名。";
    }

    @Override
    public String getInvalidJobStatusTransaction() {
        return "无法 %s 状态为 %s 的任务 \"%s\"。";
    }

    // Punctuations
    @Override
    public String getCOMMA() {
        return "，";
    }

    @Override
    public String getREC_LIST_OUT_OF_DATE() {
        return "由于优化建议所依赖的内容被删除，该优化建议已失效。请刷新页面后再试。";
    }

    @Override
    public String getGROUP_UUID_NOT_EXIST() {
        return "无法操作用户组 (UUID:%s)。请检查后重试。";
    }

    @Override
    public String getMODEL_ONLINE_WITH_EMPTY_SEG() {
        return "该模型尚未添加 Segment，不可服务于查询。请先添加 Segment 后再上线。";
    }

    @Override
    public String getMODEL_ONLINE_FORBIDDEN() {
        return "无法上线该模型。若需上线，请将配置项 “kylin.model.offline“ 设为 false。";
    }

    // multi level partition mapping
    @Override
    public String getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID() {
        return "无法更新多级分区列映射关系，参数 “multi_partition_columns“ 的值和模型中定义的多级分区列不一致。请检查后重试。";
    }

    @Override
    public String getSCD2_MODEL_ONLINE_WITH_SCD2_CONFIG_OFF() {
        return "该模型因存在 ≥ 或 < 的连接条件，当前不可上线。请删除相应连接条件，或在项目设置中开启支持拉链表开关";
    }

    @Override
    public String getCONNECT_DATABASE_ERROR() {
        return "当前无法连接 RDBMS 元数据库。请检查元数据库是否工作正常。";
    }

    // acl
    @Override
    public String getInvalidColumnAccess() {
        return "当前用户或用户组没有权限访问列 “%s“ 。";
    }

    @Override
    public String getInvalidSensitiveDataMaskColumnType() {
        return "暂不支持对 Boolean, Map, Array 类型的数据进行脱敏。";
    }

    public String getNotSupportNestedDependentCol() {
        return "无法对列 “%s” 设置关联规则，因为该列已被其他列关联。";
    }

    @Override
    public String getInvalidRowACLUpdate() {
        return "请求中包含无效的 “rows” 或 “like_rows” 参数。请使用参数 “row_filter” 进行行级权限的更新。";
    }

    // Snapshots
    @Override
    public String getSNAPSHOT_OPERATION_PERMISSION_DENIED() {
        return "没有权限操作此快照。请确保您有该快照对应的表的相关权限。";
    }

    @Override
    public String getSNAPSHOT_NOT_FOUND() {
        return "无法找到快照 “%s”。请检查后重试。";
    }

    @Override
    public String getSNAPSHOT_MANAGEMENT_NOT_ENABLED() {
        return "快照管理模式未开启。请检查后重试。";
    }

    @Override
    public String getINVALID_DIAG_TIME_PARAMETER() {
        return "终止时间必须大于起始时间。请修改。";
    }

    @Override
    public String getPARTITIONS_TO_BUILD_CANNOT_BE_EMPTY(List<String> tableDescNames) {
        return "在执行自定义分区刷新时，请为以下快照选取至少一个分区值： " + tableDescNames.toString();
    }

    // Resource Group
    @Override
    public String getRESOURCE_GROUP_FIELD_IS_NULL() {
        return "无法完成该请求。请确保资源组请求需要的所有参数都已填写完整。";
    }

    @Override
    public String getRESOURCE_CAN_NOT_BE_EMPTY() {
        return "当资源组模式开启后，请确保至少存在一个资源组。";
    }

    @Override
    public String getEMPTY_RESOURCE_GROUP_ID() {
        return "资源组 ID 不能为空。请检查后重试。";
    }

    @Override
    public String getDUPLICATED_RESOURCE_GROUP_ID(String entityId) {
        return String.format(Locale.ROOT, "资源组 ID “%s“ 已存在。请检查后重试。", entityId);
    }

    @Override
    public String getRESOURCE_GROUP_DISABLED_WITH_INVLIAD_PARAM() {
        return "如需关闭资源组模式，请先移除资源组关联的实例和项目。";
    }

    @Override
    public String getPROJECT_WITHOUT_RESOURCE_GROUP() {
        return "当前项目未绑定资源组，无法正常使用。请联系管理员进行绑定。";
    }

    @Override
    public String getEMPTY_KYLIN_INSTANCE_IDENTITY() {
        return "请填写参数 ”Instance” 的值。";
    }

    @Override
    public String getEMPTY_KYLIN_INSTANCE_RESOURCE_GROUP_ID() {
        return "请填写参数 “resource_group_id” 的值。";
    }

    @Override
    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_KYLIN_INSTANCE(String id) {
        return String.format(Locale.ROOT, "无法在实例中找到值为 “%s” 的 “resource_group_id“。请检查后重试。", id);
    }

    @Override
    public String getDUPLICATED_KYLIN_INSTANCE() {
        return "存在重复的实例。请检查后重试。";
    }

    @Override
    public String getEMPTY_PROJECT_IN_MAPPING_INFO() {
        return "在 mapping_info 中，项目不可为空。请检查后重试。";
    }

    @Override
    public String getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO() {
        return "在 mapping_info 中，参数 “resource_group_id” 不能为空。请检查后重试。";
    }

    @Override
    public String getPROJECT_BINDING_RESOURCE_GROUP_INVALID() {
        return "无法绑定项目 “%s” 的资源请求。请确保一个项目最多绑定两个资源组，且构建和查询请求各绑定一个资源组。";
    }

    @Override
    public String getMODEL_IS_NOT_MLP() {
        return "模型 “%s“ 未设置多级分区。请检查后重试。";
    }

    @Override
    public String getINVALID_PARTITION_VALUE() {
        return "子分区值 “%s” 不存在，请检查后重试。";
    }

    @Override
    public String getPARTITION_VALUE_NOT_SUPPORT() {
        return "模型 “%s” 未设置子分区列。请设置后重试。";
    }

    @Override
    public String getCONCURRENT_SUBMIT_JOB_LIMIT() {
        return "无法提交构建任务，单次最多可提交 %s 个构建任务。请尝试分批提交。";
    }

    @Override
    public String getADMIN_PERMISSION_UPDATE_ABANDON() {
        return "管理员不支持被更新权限。";
    }

    @Override
    public String getMODEL_ID_NOT_EXIST() {
        return "模型 ID “%s“ 不存在。";
    }

    @Override
    public String getNot_IN_EFFECTIVE_COLLECTION() {
        return "“%s“ 不是有效的值。该参数仅支持 “ONLINE”, “OFFLINE”, “WARNING”, “BROKEN”。";
    }

    @Override
    public String getROW_ACL_NOT_STRING_TYPE() {
        return "LIKE 行级权限仅支持 char 或 varchar 类型维度，请检查后重试。";
    }

    @Override
    public String getExceedMaxAllowedPacket() {
        return "MySQL 元数据库返回结果超过配置限制。请联系管理员在 MySQL 中将配置 “max_allowed_packet” 调整至 256M。";
    }

    @Override
    public String getRowFilterExceedLimit() {
        return "过滤器总数超过上限 (%s/%s)，请检查后重试。";
    }

    @Override
    public String getRowFilterItemExceedLimit() {
        return "过滤器包含的值超过上限 (%s/%s)，请检查后重试。";
    }

    @Override
    public String getQUERY_HISTORY_COLUMN_META() {
        return "查询开始时间,查询耗时,查询 ID,SQL 语句,查询对象,查询状态,查询节点,查询用户,查询信息\n";
    }

    @Override
    public String getSECOND_STORAGE_JOB_EXISTS() {
        return "当前无法关闭分层存储。模型 “%s” 存在正在运行的任务，请检查后再试。\n";
    }

    @Override
    public String getSECOND_STORAGE_CONCURRENT_OPERATE() {
        return "存在相关的分层存储的任务正在运行，请稍后重试。";
    }

    @Override
    public String getSECOND_STORAGE_PROJECT_JOB_EXISTS() {
        return "当前无法关闭分层存储。项目 “%s” 存在正在运行的任务，请检查后再试。\n";
    }

    @Override
    public String getSECOND_STORAGE_PROJECT_ENABLED() {
        return "加载失败，项目 %s 未开启分层存储。";
    }

    @Override
    public String getSECOND_STORAGE_MODEL_ENABLED() {
        return "加载失败，模型 %s 未开启分层存储。";
    }

    @Override
    public String getSECOND_STORAGE_SEGMENT_WITHOUT_BASE_INDEX() {
        return "Segment 中缺少基础明细索引，请添加后重试。";
    }

    @Override
    public String getJOB_RESTART_FAILED() {
        return "分层存储任务不支持重启操作。\n";
    }

    @Override
    public String getJOB_RESUME_FAILED() {
        return "分层存储任务暂时不能恢复，请稍后再试。\n";
    }

    @Override
    public String getSEGMENT_DROP_FAILED() {
        return "Segment 正在导入分层存储中。请稍后重试。\n";
    }

    @Override
    public String getINVALID_BROKER_DEFINITION() {
        return "Broker 信息不可为空，请检查后重试。";
    }

    @Override
    public String getBROKER_TIMEOUT_MESSAGE() {
        return "无法获取集群信息，请检查 Broker 信息是否正确，或确认 Kafka 服务器状态是否正常。";
    }

    @Override
    public String getSTREAMING_TIMEOUT_MESSAGE() {
        return "无法获取样例数据，请检查后重试";
    }

    @Override
    public String getEMPTY_STREAMING_MESSAGE() {
        return "该 Topic 无可展示的样例数据，请尝试换一个。";
    }

    @Override
    public String getINVALID_STREAMING_MESSAGE_TYPE() {
        return "无效的消息类型，当前仅支持 Json 或 Binary 格式的消息。请检查后重试。";
    }

    @Override
    public String getPARSE_STREAMING_MESSAGE_ERROR() {
        return "解析器无法解析样例数据，建议检查选项设置或者修改解析器后再尝试解析。";
    }

    @Override
    public String getREAD_KAFKA_JAAS_FILE_ERROR() {
        return "无法正确读取 Kafka 认证文件，请检查后再试。";
    }

    @Override
    public String getBATCH_STREAM_TABLE_NOT_MATCH() {
        return "表 “%s” 与 Kafka 表的列不一致，请确认两者的列完全一致后重试。";
    }

    public String getSTREAMING_INDEXES_DELETE() {
        return "无法删除流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    public String getSTREAMING_INDEXES_EDIT() {
        return "无法编辑流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    public String getSTREAMING_INDEXES_ADD() {
        return "无法添加流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    public String getSTREAMING_INDEXES_APPROVE() {
        return "流数据模型暂无法通过优化建议。";
    }

    public String getSTREAMING_INDEXES_CONVERT() {
        return "流数据模型暂无法转换为优化建议。";
    }

    @Override
    public String getCANNOT_FORCE_TO_BOTH_PUSHDODWN_AND_INDEX() {
        return "不能同时强制下推和击中模型，参数 “forcedToPushDown” 和 “forced_to_index” 不能同时使用。请检查后重试。";
    }

    @Override
    public String getSECOND_STORAGE_NODE_NOT_AVAILABLE() {
        return "无法添加节点。节点不存在或被其他项目占用，请修改后重试";
    }

    @Override
    public String getBASE_TABLE_INDEX_NOT_AVAILABLE() {
        return "当前无法开启分层存储。请先创建基础所明细索引。";
    }

    @Override
    public String getPARTITION_COLUMN_NOT_AVAILABLE() {
        return "当前无法开启分层存储。请将时间分区列添加到维度，并更新基础明细索引。";
    }

    @Override
    public String getPROJECT_LOCKED() {
        return "当前项目中有正在进行的分层存储重分布任务，请稍后再试。";
    }

    @Override
    public String getFIX_STREAMING_SEGMENT() {
        return "无法修复流数据模型的 Segment。";
    }

    @Override
    public String getStreamingDisabled() {
        return "只有 Kyligence 高级版才能使用批流一体功能，请联系 Kyligence 客户经理升级 License。";
    }

    @Override
    public String getNO_STREAMING_MODEL_FOUND() {
        return "无法查询。由于流数据必须通过索引查询，请确保有相应的索引。";
    }

    @Override
    public String getSTREAMING_TABLE_NOT_SUPPORT_AUTO_MODELING() {
        return "不支持流数据表进行自动建模。";
    }

    @Override
    public String getSPARK_FAILURE() {
        return "无法完成操作，请检查 Spark 环境后重试。";
    }

    @Override
    public String getDOWNLOAD_QUERY_HISTORY_TIMEOUT() {
        return "导出超时，请稍后重试。";
    }

    @Override
    public String getSTREAMING_OPERATION_NOT_SUPPORT() {
        return "API调用失败，暂不支持调用流数据相关的API。";
    }

    @Override
    public String getJDBC_CONNECTION_INFO_WRONG() {
        return "连接信息错误，请检查后重试。";
    }

    @Override
    public String getJDBC_NOT_SUPPORT_PARTITION_COLUMN_IN_SNAPSHOT() {
        return "当前数据源的 Snapshot 无法使用分区设置。";
    }

    @Override
    public String getParamTooLarge() {
        return "参数 '%s' 太长， 最大 %s 字节。";
    }

    // KAP query sql blacklist
    public String getSQL_BLACKLIST_ITEM_ID_EMPTY() {
        return "黑名单条目 id 不能为空";
    }

    public String getSQL_BLACKLIST_ITEM_REGEX_AND_SQL_EMPTY() {
        return "黑名单条目正则表达式和 sql 不能都为空";
    }

    public String getSQL_BLACKLIST_ITEM_PROJECT_EMPTY() {
        return "黑名单所属项目不能为空";
    }

    public String getSQL_BLACKLIST_ITEM_ID_EXISTS() {
        return "黑名单条目 id 不能为空";
    }

    public String getSQL_BLACKLIST_ITEM_ID_NOT_EXISTS() {
        return "黑名单条目 id 已存在";
    }

    public String getSQL_BLACKLIST_ITEM_REGEX_EXISTS() {
        return "黑名单条目正则表达式已存在";
    }

    public String getSQL_BLACKLIST_ITEM_SQL_EXISTS() {
        return "黑名单条目 sql 已存在";
    }

    public String getSQL_BLACKLIST_ITEM_ID_TO_DELETE_EMPTY() {
        return "待删除的黑名单条目 id 不能为空";
    }

    public String getSQL_BLACKLIST_QUERY_REJECTED() {
        return "查询被黑名单终止, 黑名单条目 id: %s.";
    }

    public String getSQL_BLACKLIST_QUERY_CONCUTTENT_LIMIT_EXCEEDED() {
        return "查询被黑名单终止，因为超出了并发限制, 黑名单条目 id: %s, 并发限制: {%s}";
    }

    public String getINVALID_RANGE() {
        return "%s 不是 [%s - %s] 范围内整数";
    }

    @Override
    public String getLDAP_USER_DATA_SOURCE_CONNECTION_FAILED() {
        return "LDAP服务异常，请检查用户数据源。";
    }

    @Override
    public String getLDAP_USER_DATA_SOURCE_CONFIG_ERROR() {
        return "LDAP 连接错误，请检查 LDAP 配置信息！";
    }

    @Override
    public String getTABLE_NO_COLUMNS_PERMISSION() {
        return "请向表中的列添加权限! ";
    }

    @Override
    public String getPARAMETER_IS_REQUIRED() {
        return "找不到 '%s'。";
    }
}
