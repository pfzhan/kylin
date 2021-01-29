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
        return "当前模型下，可计算列名[%s]已被使用，请重新命名可计算列名";
    }

    @Override
    public String getCUBE_NOT_FOUND() {
        return "找不到 Cube '%s'";
    }

    @Override
    public String getSEG_NOT_FOUND() {
        return "找不到 Segment '%s' 在模型 '%s' 上";
    }

    @Override
    public String getKAFKA_DEP_NOT_FOUND() {
        return "找不到 Kafka 依赖";
    }

    @Override
    public String getBUILD_DRAFT_CUBE() {
        return "Cube 草稿不能被构建";
    }

    @Override
    public String getBUILD_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被构建";
    }

    @Override
    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    }

    public String getDELETE_NOT_FIRST_LAST_SEG() {
        return "非首尾 segment '%s' 不能被删除";
    }

    @Override
    public String getDELETE_NOT_READY_SEG() {
        return "非 READY 状态 segment '%s' 不能被删除, 请先抛弃它正在运行的任务";
    }

    @Override
    public String getINVALID_BUILD_TYPE() {
        return "非法构建类型: '%s'";
    }

    @Override
    public String getNO_ACL_ENTRY() {
        return "找不到对象 '%s' 的授权记录";
    }

    @Override
    public String getACL_INFO_NOT_FOUND() {
        return "找不到对象 '%s' 的授权信息";
    }

    @Override
    public String getACL_DOMAIN_NOT_FOUND() {
        return "找不到授权对象";
    }

    @Override
    public String getPARENT_ACL_NOT_FOUND() {
        return "找不到上级授权";
    }

    @Override
    public String getDISABLE_NOT_READY_CUBE() {
        return "仅 ready 状态的 cube 可以被禁用, '%s' 的状态是 %s";
    }

    @Override
    public String getPURGE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被清空, '%s' 的状态是 %s";
    }

    @Override
    public String getCLONE_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被克隆";
    }

    @Override
    public String getINVALID_CUBE_NAME() {
        return "非法 cube 名称 '%s', 仅支持字母, 数字和下划线";
    }

    @Override
    public String getCUBE_ALREADY_EXIST() {
        return "Cube 名称 '%s' 已存在";
    }

    @Override
    public String getCUBE_DESC_ALREADY_EXIST() {
        return "Cube '%s' 已存在";
    }

    @Override
    public String getBROKEN_CUBE_DESC() {
        return "损坏的 Cube 描述 '%s'";
    }

    @Override
    public String getENABLE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被启用, '%s' 的状态是 %s";
    }

    @Override
    public String getNO_READY_SEGMENT() {
        return "Cube '%s' 不包含任何 READY 状态的 segment";
    }

    @Override
    public String getENABLE_WITH_RUNNING_JOB() {
        return "Cube 存在正在运行的任务, 不能被启用";
    }

    @Override
    public String getDISCARD_JOB_FIRST() {
        return "Cube '%s' 存在正在运行或失败的任务, 请抛弃它们后重试";
    }

    @Override
    public String getIDENTITY_EXIST_CHILDREN() {
        return "'%s' 存在下级授权";
    }

    @Override
    public String getINVALID_CUBE_DEFINITION() {
        return "非法 cube 定义";
    }

    @Override
    public String getEMPTY_CUBE_NAME() {
        return "Cube 名称不可为空";
    }

    @Override
    public String getUSE_DRAFT_MODEL() {
        return "不能使用模型草稿 '%s'";
    }

    @Override
    public String getINCONSISTENT_CUBE_DESC() {
        return "Cube 描述 '%s' 与现有不一致， 请清理 cube 或避免更新 cube 描述的关键字段";
    }

    @Override
    public String getUPDATE_CUBE_NO_RIGHT() {
        return "无权限更新此 cube";
    }

    @Override
    public String getNOT_STREAMING_CUBE() {
        return "Cube '%s' 不是实时 cube";
    }

    @Override
    public String getCUBE_RENAME() {
        return "Cube 不能被重命名";
    }

    // Model
    @Override
    public String getINVALID_MODEL_DEFINITION() {
        return "非法模型定义";
    }

    @Override
    public String getEMPTY_MODEL_NAME() {
        return "模型名称不可为空";
    }

    @Override
    public String getINIT_MEASURE_FAILED() {
        return "度量 %s 初始化失败：%s";
    }

    @Override
    public String getINVALID_MODEL_NAME() {
        return "非法模型名称 '%s', 仅支持字母, 数字和下划线";
    }

    @Override
    public String getINVALID_DIMENSION_NAME() {
        return "'%s'维度名称无效， 支持中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getINVALID_MEASURE_NAME() {
        return "'%s'度量名称无效， 支持中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getMODEL_ID_NOT_FOUND() {
        return "模型 ID 不能为空";
    }

    @Override
    public String getDUPLICATE_MODEL_NAME() {
        return "模型名称 '%s' 已存在, 不能被创建";
    }

    @Override
    public String getDROP_REFERENCED_MODEL() {
        return "模型被 Cube '%s' 引用, 不能被删除";
    }

    @Override
    public String getUPDATE_MODEL_KEY_FIELD() {
        return "由于维度、度量或者连接关系被修改导致与存在的cube定义不一致，因而当前模型无法保存。";
    }

    @Override
    public String getBROKEN_MODEL_OPERATION_DENIED() {
        return "无法操作 BROKEN 模型 “%s”。";
    }

    @Override
    public String getMODEL_NOT_FOUND() {
        return "找不到模型 '%s'";
    }

    @Override
    public String getMODEL_MODIFY_ABANDON(String table) {
        return String.format(Locale.ROOT, "模型不支持被修改，因为没有 ['%s'] 权限", table);
    }

    @Override
    public String getINDEX_ALREADY_DELETED() {
        return "该索引已经被删除";
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
    public String getEMPTY_NEW_MODEL_NAME() {
        return "新模型名称不可为空";
    }

    @Override
    public String getUPDATE_MODEL_NO_RIGHT() {
        return "无权限更新此模型";
    }

    @Override
    public String getMODEL_RENAME() {
        return "模型不能被重命名";
    }

    @Override
    public String getDUPLICATE_DIMENSION_NAME() {
        return "维度名称 '%s' 已存在";
    }

    @Override
    public String getDUPLICATE_MEASURE_NAME() {
        return "度量名称 '%s' 已存在";
    }

    @Override
    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "维度表达 '%s' 已存在，不能被创建";
    }

    @Override
    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "Join条件 '%s'和'%s' 已存在，不能被创建";
    }

    @Override
    public String getCheckCCType() {
        return "可计算列 {0} 定义的数据类型 {2} 与实际类型{1} 不符，请修改后进行重试";
    }

    @Override
    public String getCheckCCExpression() {
        return "可计算列 %s 表达式 %s 校验失败。";
    }

    @Override
    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "解析失败，请检查模型数据包是否完整。";
    }

    @Override
    public String getCOMPUTED_COLUMN_EXPRESSION_ALREADY_DEFINED() {
        return "模型 '%s' 导入失败：该模型的可计算列 '%s' 与模型 '%s' 中的可计算列 '%s' 表达式相同。";
    }

    @Override
    public String getEXPORT_BROKEN_MODEL() {
        return "无法导出 Broken 的模型 [%s]。";
    }

    @Override
    public String getIMPORT_BROKEN_MODEL() {
        return "无法导入 Broken 的模型 [%s]。";
    }

    @Override
    public String getIMPORT_MODEL_EXCEPTION() {
        return "模型导入失败。";
    }

    @Override
    public String getUN_SUITABLE_IMPORT_TYPE() {
        return "模型 [%s] 的导入类型 [%s] 不合法。";
    }

    @Override
    public String getCAN_NOT_OVERWRITE_MODEL() {
        return "模型 [%s] 不存在，不能够覆盖。";
    }

    @Override
    public String getILLEGAL_MODEL_METADATA_FILE() {
        return "导入的元数据文件不合法。";
    }

    @Override
    public String getEXPORT_AT_LEAST_ONE_MODEL() {
        return "至少需要选择一个模型导出。";
    }

    @Override
    public String getCOMPUTED_COLUMN_NAME_ALREADY_DEFINED() {
        return "模型 '%s' 导入失败：该模型的可计算列 '%s' 与模型 '%s' 中的可计算列重名。";
    }

    @Override
    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED() {
        return "该可计算列的表达式已被用于模型 '%s'，名称为 '%s'。请修改名称以保持一致，或使用其他的表达式。";
    }

    @Override
    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED_SINGLE_MODEL() {
        return "该表达式在模型内已存在。";
    }

    @Override
    public String getCOMPUTED_COLUMN_NAME_DUPLICATED() {
        return "可计算列名称 '%s' 已被用于模型 '%s'，表达式为 '%s'。请修改表达式以保持一致，或使用其他的名称。";
    }

    @Override
    public String getCOMPUTED_COLUMN_NAME_DUPLICATED_SINGLE_MODEL() {
        return "该可计算列名在模型内已存在。";
    }

    @Override
    public String getFACT_TABLE_USED_AS_LOOK_UP_TABLE() {
        return "模型 '%s' 导入失败：模型中的事实表在其他模型中已经作为维度表存在。";
    }

    @Override
    public String getMODEL_METADATA_CHECK_FAILED() {
        return "压缩包中包含的模型元数据与系统已有的模型元数据存在冲突，请查看详细冲突信息，修正后重新导入。";
    }

    @Override
    public String getMODEL_CHANGE_PERMISSION() {
        return "仅系统管理员和项目管理员可以更改模型的所有者。";
    }

    @Override
    public String getMODEL_OWNER_CHANGE_INVALID_USER() {
        return "非法用户！仅系统管理员、项目的管理员角色和 Management 角色可以被设置成模型的所有者。";
    }

    // index
    @Override
    public String getINDEX_STATUS_TYPE_ERROR() {
        return "参数“status”仅支持传入 NO_BUILD，ONLINE，LOCKED，BUILDING。";
    }

    @Override
    public String getINDEX_SOURCE_TYPE_ERROR() {
        return "参数“sources”仅支持传入 RECOMMENDED_AGG_INDEX，RECOMMENDED_TABLE_INDEX，CUSTOM_AGG_INDEX，CUSTOM_TABLE_INDEX。";
    }

    @Override
    public String getINDEX_SORT_BY_ERROR() {
        return "参数“sort_by”仅支持传入 last_modified，usage，data_size。";
    }

    // Job
    @Override
    public String getILLEGAL_TIME_FILTER() {
        return "非法时间条件: %s";
    }

    @Override
    public String getILLEGAL_EXECUTABLE_STATE() {
        return "非法状态: %s";
    }

    @Override
    public String getILLEGAL_JOB_TYPE() {
        return "非法任务类型, id: %s.";
    }

    @Override
    public String getILLEGAL_STATE_TRANSFER() {
        return "非法的任务状态转移, id: [%s], 转移前: [%s], 转移后: [%s]";
    }

    @Override
    public String getINVALID_PRIORITY() {
        return "无效优先级，必须在0-4范围内";
    }

    // Acl
    @Override
    public String getUSER_NOT_EXIST() {
        return "用户 '%s' 不存在, 请确认用户是否存在。";
    }

    @Override
    public String getUSERGROUP_EXIST() {
        return "用户组 [%s] 已存在。";
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
        return "该项目名 \"%s\" 已存在。请重新命名。";
    }

    @Override
    public String getPROJECT_NOT_FOUND() {
        return "无法找到项目 \"%s\"。";
    }

    @Override
    public String getSQL_LIST_IS_EMPTY() {
        return "请输入数组参数 “sqls”。";
    }

    @Override
    public String getDELETE_PROJECT_NOT_EMPTY() {
        return "不能修改该项目，如需要修改请先清空其中的Cube和Model";
    }

    public String getRENAME_PROJECT_NOT_EMPTY() {
        return "不能重命名该项目，如果要重命名请先清空其中的Cube和Model";
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

    @Override
    public String getTABLE_DESC_NOT_FOUND() {
        return "找不到表 '%s'";
    }

    @Override
    public String getTABLE_IN_USE_BY_MODEL() {
        return "表已被模型 '%s' 使用";
    }

    // table sampling
    @Override
    public String getBEYOND_MIX_SAMPLING_ROWSHINT() {
        return "采样的行数低于了最小采样行数（ %d 行）";
    }

    @Override
    public String getBEYOND_MAX_SAMPLING_ROWS_HINT() {
        return "采样的行数超过了最大采样行数（ %d 行）";
    }

    @Override
    public String getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME() {
        return "采样表的名称不符合规范，表名正确格式：database.table";
    }

    @Override
    public String getFAILED_FOR_IN_SAMPLING() {
        return "表 %s 有正在进行的抽样任务，暂时无法触发新一个抽样任务。";
    }

    @Override
    public String getFAILED_FOR_NO_SAMPLING_TABLE() {
        return "没有传入采样表名称（database.table），请至少提供一张表";
    }

    @Override
    public String getRELOAD_TABLE_CC_RETRY() {
        return "%s源表 %s 中列 %s 的数据类型发生了变更。请删除可计算列或修改数据类型后再试。";
    }

    @Override
    public String getRELOAD_TABLE_MODEL_RETRY() {
        return "源表 %1$s 中列 %2$s 的数据类型发生变更。请从模型 %3$s 中删除该列，或修改该列的数据类型。";
    }

    // Cube Desc
    @Override
    public String getCUBE_DESC_NOT_FOUND() {
        return "找不到 cube '%s'";
    }

    // Streaming
    @Override
    public String getINVALID_TABLE_DESC_DEFINITION() {
        return "非法表定义";
    }

    @Override
    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return "非法 StreamingConfig 定义";
    }

    @Override
    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "非法 KafkaConfig 定义";
    }

    @Override
    public String getADD_STREAMING_TABLE_FAIL() {
        return "添加流式表失败";
    }

    @Override
    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return "StreamingConfig 名称不可为空";
    }

    @Override
    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return "StreamingConfig '%s' 已存在";
    }

    @Override
    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return "保存 StreamingConfig 失败";
    }

    @Override
    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return "KafkaConfig '%s' 已存在";
    }

    @Override
    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return "StreamingConfig 已创建, 但 KafkaConfig 创建失败";
    }

    @Override
    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return "KafkaConfig 保存失败";
    }

    @Override
    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 StreamingConfig 失败";
    }

    @Override
    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 KafkaConfig 失败";
    }

    @Override
    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return "无权限更新此 StreamingConfig";
    }

    @Override
    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return "无权限更新此 KafkaConfig";
    }

    @Override
    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return "找不到 StreamingConfig '%s'";
    }

    @Override
    public String getQUERY_NOT_ALLOWED() {
        return "任务节点不支持查询";
    }

    @Override
    public String getNOT_SUPPORTED_SQL() {
        return "不支持的 SQL";
    }

    @Override
    public String getTABLE_META_INCONSISTENT() {
        return "表元数据与JDBC 元数据不一致";
    }

    @Override
    public String getCOLUMN_META_INCONSISTENT() {
        return "列元数据与JDBC 元数据不一致";
    }

    @Override
    public String getDUPLICATE_QUERY_NAME() {
        return "查询名称重复 '%s'";
    }

    @Override
    public String getNULL_EMPTY_SQL() {
        return "SQL不能为空";
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
    public String getINVALID_SID() {
        return "用户名/组名只能包含字母，数字，空格和下划线";
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
    public String getACE_ID_REQUIRED() {
        return "找不到 Ace id";
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
        return "该查询不存在有效的结果, 请首先检查它的状态";
    }

    @Override
    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "查询结果文件不存在";
    }

    @Override
    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "查询异常文件不存在";
    }

    @Override
    public String getCLEAN_FOLDER_FAIL() {
        return "清理文件夹失败";
    }

    @Override
    public String getASYNC_QUERY_TIME_FORMAT_ERROR() {
        return "无效的时间格式。请按 yyyy-MM-dd HH:mm:ss 格式填写。";
    }

    // Admin
    @Override
    public String getGET_ENV_CONFIG_FAIL() {
        return "无法获取 Kylin env Config";
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
        return "用户名或密码错误，请在 " + formatSeconds(seconds) + " 后再次重试。";
    }

    @Override
    public String getUSER_IN_LOCKED_STATUS(long leftSeconds, long nextLockSeconds) {
        return "用户 %s 已被锁定，请在 " + formatSeconds(leftSeconds) + " 后重试。" + formatNextLockDuration(nextLockSeconds);
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
        return "用户名或密码错误。";
    }

    @Override
    public String getUSER_LOGIN_AS_USER_NOT_ADMIN() {
        return "仅 ADMIN 用户可切换为其他用户登录。";
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

    // Diagnosis
    @Override
    public String getDIAG_NOT_FOUND() {
        return "在 %s 找不到 diag.sh";
    }

    @Override
    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return "无法生成诊断包";
    }

    @Override
    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "诊断包不可用, 路径: %s";
    }

    @Override
    public String getDIAG_PACKAGE_NOT_FOUND() {
        return "找不到诊断包, 路径: %s";
    }

    @Override
    public String getDIAG_FAILED() {
        return "生成诊断包失败。请尝试重新生成。";
    }

    // ExternalFilter
    @Override
    public String getFILTER_ALREADY_EXIST() {
        return "Filter '%s' 已存在";
    }

    @Override
    public String getFILTER_NOT_FOUND() {
        return "找不到 filter '%s'";
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
    public String getSQL_NUMBER_EXCEEDS_LIMIT() {
        return "最多可同时导入 %s 条 SQL";
    }

    @Override
    public String getSQL_FILE_TYPE_MISMATCH() {
        return "sql文件的后缀必须是 'txt' 或 'sql'";
    }

    // Query statistics

    @Override
    public String getNOT_SET_INFLUXDB() {
        return "未设置参数 kap.metric.write-destination 为 INFLUX";
    }

    //license

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
        return "当前已使用数据量（%s/%s）和节点数（%s/%s）均超过许可证上限。系统无法进行构建或数据加载任务。\n"
                + "请联系 Kyligence 销售人员，或尝试停止部分节点并且删除一些 Segment并停止部分节点以解除限制。";
    }

    @Override
    public String getLICENSE_PROJECT_SOURCE_NODES_OVER_CAPACITY() {
        return "当前项目已使用数据量（%s/%s）和节点数（%s/%s）均超过配置上限。系统无法进行构建或数据加载任务。\n"
                + "请联系 Kyligence 销售人员，或尝试停止部分节点并且删除一些 Segment并停止部分节点以解除限制。";
    }

    // ICredential

    @Override
    public String getINVALID_CREDENTIAL() {
        return "错误凭证,请检查您的凭证";
    }

    @Override
    public String getINVALID_URL() {
        return "错误文件地址，请检查您的文件地址";
    }

    @Override
    public String getNoTableFound() {
        return "找不到表。请检查您的SQL语句";
    }

    @Override
    public String getNoJobNode() {
        return "未发现执行任务的节点。请启动对应节点或配置一个任务节点（ 配置文件${KYLIN_HOME}/conf/kylin.properties，配置项 kylin.server.mode）。";
    }

    @Override
    public String getTABLENOTFOUND() {
        return "模型 [%s] 保存失败，请确保模型中使用的列 [%s] 在源表 [%s] 中存在";
    }

    // Async push down get date format
    @Override
    public String getPUSHDOWN_PARTITIONFORMAT_ERROR() {
        return "获取失败，请手动选择分区格式";
    }

    // Async push down get data range
    @Override
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "获取失败，请手动选择数据范围";
    }

    @Override
    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "获取超时，请手动选择数据范围";
    }

    @Override
    public String getDIMENSION_NOTFOUND() {
        return "维度 %s 正在被聚合组、系统推荐的聚合索引或明细索引引用。请将该维度从相关聚合组中删除，或删除相关索引。";
    }

    @Override
    public String getMEASURE_NOTFOUND() {
        return "度量 %s 正在被索引引用。请在聚合组或明细索引中删除对应字段后再进行操作。";
    }

    @Override
    public String getNESTED_CC_CASCADE_ERROR() {
        return "操作失败，在当前模型中存在嵌套可计算列[%s]依赖于当前可计算列[%s]，嵌套可计算列的表达式为[%s]。";
    }

    @Override
    public String getCHANGE_GLOBALADMIN() {
        return "您不可以添加，修改，删除系统管理员的权限。";
    }

    @Override
    public String getCHANGE_DEGAULTADMIN() {
        return "由于用户ADMIN是系统默认内置管理员，所以不可以移除ROLE_ADMIN权限，删除，禁用用户ADMIN，并且仅用户ADMIN才可以更改用户ADMIN的密码和用户组";
    }

    //Query
    @Override
    public String getINVALID_USER_TAG() {
        return "user_defined_tag 必须小于等于 256.";
    }

    @Override
    public String getINVALID_ID() {
        return "无法找到 ID \"%s\"。请检查后重试。";
    }

    @Override
    public String getSEGMENT_LOCKED() {
        return "由于 Segment %s 被锁定，所以不能删除、刷新或者合并该 Segment。";
    }

    @Override
    public String getSEGMENT_STATUS(String status) {
        return "由于 Segment %s 处于 " + status + " 状态，所以不能合并或者刷新该 Segment。";
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
        return "系统中暂无活跃的任务节点，请联系您的系统管理员进行检查并修复。";
    }

    @Override
    public String getLEADERS_HANDLE_OVER() {
        return "系统正在尝试恢复服务，请稍后进行尝试。";
    }

    @Override
    public String getTABLE_REFRESH_NOTFOUND() {
        return "未找到缓存表: %s";
    }

    @Override
    public String getTABLE_REFRESH_ERROR() {
        return "刷新缓存表异常";
    }

    @Override
    public String getTABLE_REFRESH_PARAM_INVALID() {
        return "请求中tables字段非法，请检查后重试.";
    }

    @Override
    public String getTABLE_REFRESH_PARAM_NONE() {
        return "请求中未发现tables字段，请检查后重试.";
    }

    @Override
    public String getTABLE_REFRESH_PARAM_MORE() {
        return "请求中包含非tables的多余字段，请检查后重试.";
    }

    @Override
    public String getTRANSFER_FAILED() {
        return "转发失败。";
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
        return "列[%s]不是一个维度";
    }

    @Override
    public String getMODEL_CAN_NOT_PURGE() {
        return "模型[%s]无法被清除";
    }

    @Override
    public String getMODEL_SEGMENT_CAN_NOT_REMOVE() {
        return "模型[%s]不能手动清除Segment";
    }

    @Override
    public String getSEGMENT_CAN_NOT_REFRESH() {
        return "由于您需要刷新的Segment中有部分Segment正在构建，您现在无法刷新。";
    }

    @Override
    public String getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE() {
        return "可用的Segment范围已更改，无法刷新，请重试。";
    }

    @Override
    public String getCAN_NOT_BUILD_SEGMENT() {
        return "无法构建Segment，请先定义聚合索引或者明细索引。";
    }

    @Override
    public String getCAN_NOT_BUILD_SEGMENT_MANUALLY() {
        return "模型[%s]不能手动构建Segment";
    }

    @Override
    public String getCAN_NOT_BUILD_INDICES_MANUALLY() {
        return "模型[%s]不能手动构建索引";
    }

    @Override
    public String getINVALID_REMOVE_SEGMENT() {
        return "只有头部或尾部的连续的Segment才能被移除！";
    }

    @Override
    public String getINVALID_MERGE_SEGMENT() {
        return "无法合并暂不可用的Segment";
    }

    @Override
    public String getINVALID_SET_TABLE_INC_LOADING() {
        return "无法设置表[％s]的增量加载，因为另一个模型[％s]将该表用作维表";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT() {
        return "没有可用的Segment可以刷新";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT_BY_NOT_READY() {
        return "刷新范围内的数据必须就绪";
    }

    @Override
    public String getINVALID_LOAD_HIVE_TABLE_NAME() {
        return "不能执行该操作，请设置kap.table.load-hive-tablename-cached.enabled=true，然后重试";
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
        return "并发查询请求太多。";
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
        return "Segments 列表为空";
    }

    @Override
    public String getSEGMENT_ID_NOT_EXIST() {
        return "Segment Ids [%s] 不存在。";
    }

    @Override
    public String getSEGMENT_NAME_NOT_EXIST() {
        return "Segment Names [%s] 不存在。";
    }

    @Override
    public String getLAYOUT_LIST_IS_EMPTY() {
        return "Layouts 列表为空";
    }

    @Override
    public String getLAYOUT_NOT_EXISTS() {
        return "Layouts [%s] 不存在!";
    }

    @Override
    public String getINVALID_REFRESH_SEGMENT() {
        return "您应该至少选择一个Segment来刷新。";
    }

    @Override
    public String getEMPTY_SEGMENT_PARAMETER() {
        return "请输入 Segment ID 或者 Segment Name。";
    }

    @Override
    public String getCONFLICT_SEGMENT_PARAMETER() {
        return "您不能同时输入Segment ID 和 Name。";
    }

    @Override
    public String getINVALID_MERGE_SEGMENT_BY_TOO_LESS() {
        return "您应该至少选择两个Segment来合并。";
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
        return "计算列[%s]的名称是 SQL 关键字，请使用其他名称。";
    }

    @Override
    public String getINVALID_COMPUTER_COLUMN_NAME() {
        return "计算列[%s]的名称不能为空，或以字母以外的符号开头，或包含字母、数字、下划线以外的符号，请使用其他名称。";
    }

    @Override
    public String getMODEL_ALIAS_DUPLICATED() {
        return "模型表名[%s]已存在";
    }

    @Override
    public String getINVALID_RANGE_LESS_THAN_ZERO() {
        return "起始时间和终止时间必须大于0";
    }

    @Override
    public String getINVALID_RANGE_NOT_FORMAT() {
        return "无效的起始时间或终止时间格式。仅支持时间戳类型，单位毫秒";
    }

    @Override
    public String getINVALID_RANGE_END_LESSTHAN_START() {
        return "终止时间必须大于起始时间";
    }

    @Override
    public String getINVALID_RANGE_NOT_CONSISTENT() {
        return "起始时间和终止时间必须同时存在或者同时不存在";
    }

    @Override
    public String getID_CANNOT_EMPTY() {
        return "ID不能为空";
    }

    @Override
    public String getINVALID_CREATE_MODEL() {
        return "无法在SQL加速项目中手动创建模型！";
    }

    @Override
    public String getSEGMENT_INVALID_RANGE() {
        return "要刷新的[%s] Segment 范围已经超出了加载数据的范围，加载数据的范围是[%s]";
    }

    @Override
    public String getSEGMENT_RANGE_OVERLAP() {
        return "将要构建的范围和已构建的范围重合，从[%s]到[%s]，请选择新的数据范围，然后重试";
    }

    @Override
    public String getPARTITION_COLUMN_NOT_EXIST() {
        return "分区列不存在";
    }

    @Override
    public String getINVALID_PARTITION_COLUMN() {
        return "时间分区列必须使用事实表上的原始列";
    }

    @Override
    public String getTABLE_NAME_CANNOT_EMPTY() {
        return "必须指定表名！";
    }

    @Override
    public String getTABLE_SAMPLE_MAX_ROWS() {
        return "表级数据抽样取值范围在 10000 ~ 20000000 之间";
    }

    @Override
    public String getTABLE_NOT_FOUND() {
        return "找不到表 '%s'";
    }

    @Override
    public String getILLEGAL_JOB_STATE() {
        return "非法的任务状态:%s 状态必须是 “PENDING“, “RUNNING“, “FINISHED“, “ERROR” 或者 “DISCARDED“.";
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
        return "数据库:[%s]不存在";
    }

    @Override
    public String getBROKEN_MODEL_CANNOT_ONOFFLINE() {
        return "处于Broken状态的模型[%s]无法上线或下线";
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
    public String getINVALID_NAME_CONTAINS_OTHER_CHARACTER() {
        return "用户名/用户组中仅支持英文字符";
    }

    @Override
    public String getINVALID_NAME_CONTAINS_INLEGAL_CHARACTER() {
        return "用户名/用户组名中不能包含如下符号: 反斜杠(\\), 斜杠(/), 冒号(:), 星号(*), 问号(?), 引号(“), 小于号(<), 大于号(>), 垂直线(|)";
    }

    @Override
    public String getHIVETABLE_NOT_FOUND() {
        return "数据源中以下表加载失败：{%s}。请检查数据源。";
    }

    @Override
    public String getDUPLICATE_LAYOUT() {
        return "已存在相同的索引。";
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
        return "语法错误：在 '%s' 列，'%s' 行出现无法辨识的 token：“%s”。";
    }

    @Override
    public String getBAD_SQL_REASON() {
        return "语法错误：“%s”。";
    }

    @Override
    public String getBAD_SQL_SUGGEST() {
        return "请修正 SQL。";
    }

    @Override
    public String getBAD_SQL_TABLE_NOT_FOUND_REASON() {
        return "没有找到表 “%s”。";
    }

    @Override
    public String getBAD_SQL_TABLE_NOT_FOUND_SUGGEST() {
        return "请在数据源中导入表 “%s”。如果该表已经存在，请在查询中使用数据库名.表名的形式进行引用。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_REASON() {
        return "列 “%s” 不存在。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST() {
        return "请在数据源中添加列“%s”。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON() {
        return "在表 “%s” 中没有找到列 “%s”。";
    }

    @Override
    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGESTION() {
        return "请将列 “%s” 添加到数据源表 “%s”。";
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
        return "查询失败。查询结果行数超过最大值:{%s}。请添加过滤条件或联系管理员调整最大查询结果行数。";
    }

    @Override
    public String getAGG_INDEX_LOST_DIMENSION() {
        return "聚合索引缺少依赖：缺少维度，您需要先通过对应维度的优化建议。";
    }

    @Override
    public String getAGG_INDEX_LOST_MEASURE() {
        return "聚合索引缺少依赖：缺少度量，您需要先通过对应度量的优化建议。";
    }

    @Override
    public String getTABLE_INDEX_LOST_CC() {
        return "明细索引缺少依赖：缺少可计算列，您需要先通过对应可计算列的优化建议。";
    }

    @Override
    public String getMEASURE_LOST_CC() {
        return "度量缺少依赖：缺少可计算列，您需要先通过对应可计算列的优化建议。";
    }

    @Override
    public String getCC_LOST_CC() {
        return "可计算列缺少依赖：缺少可计算列，您需要先通过对应可计算列的优化建议。";
    }

    @Override
    public String getDIMENSION_LOST_CC() {
        return "维度缺少依赖：缺少可计算列，您需要先通过对应可计算列的优化建议。";
    }

    @Override
    public String getCC_EXPRESSION_CONFLICT(String newCCExpression, String newCCName, String existedCCName) {
        return String.format(Locale.ROOT, "可计算列%s的表达式%s与可计算列%s相同。", newCCName, newCCExpression, existedCCName);
    }

    @Override
    public String getCC_COLUMN_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "可计算列名“%s”与当前模型下的列重名。请重新命名。", ccName);
    }

    @Override
    public String getCC_MEASURE_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "可计算列名“%s”与当前模型下的度量重名。请重新命名。", ccName);
    }

    @Override
    public String getCC_DIMENSION_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "可计算列名“%s”与当前模型下的维度重名。请重新命名。", ccName);
    }

    @Override
    public String getCC_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "可计算列“%s”已存在。", ccName);
    }

    @Override
    public String getDIMENSION_CONFLICT(String dimensionName) {
        return String.format(Locale.ROOT, "维度%s已存在。", dimensionName);
    }

    @Override
    public String getMEASURE_CONFLICT(String measureName) {
        return String.format(Locale.ROOT, "度量%s已存在。", measureName);
    }

    @Override
    public String getINVALID_TIME_FORMAT() {
        return "时间分区列设置失败，您选取的时间分区列不符合时间格式，请重新选择其他时间分区列。";
    }

    @Override
    public String getSEGMENT_CONTAINS_GAPS() {
        return "Segment %s 和 %s 区间不连续。";
    }

    @Override
    public String getSegmentMergeLayoutConflictError() {
        return "当前 Segments 所包含的索引不一致，请先构建索引并确保其一致后再合并。";
    }

    @Override
    public String getSegmentMergePartitionConflictError() {
        return "当前 segments 所包含的分区不一致，请先构建分区并确保其一致后再合并";
    }

    @Override
    public String getDIMENSION_TABLE_USED_IN_THIS_MODEL() {
        return "此模型维度表已被设置为事实表。请重新设置维度表。";
    }

    @Override
    public String getNO_DATA_IN_TABLE() {
        return "表[%s]中无数据。";
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
        return "重写模型设置失败，%s 参数值必须为非负整数.";
    }

    @Override
    public String getINVALID_MEMORY_SIZE() {
        return "重写模型设置失败，spark-conf.spark.executor.memory 参数值必须为非负整数与单位 g 的组合.";
    }

    @Override
    public String getINVALID_BOOLEAN_FORMAT() {
        return "重写模型设置失败，%s 参数值必须为 true 或 false.";
    }

    @Override
    public String getINVALID_AUTO_MERGE_CONFIG() {
        return "重写模型设置失败，自动合并范围不能为空.";
    }

    @Override
    public String getCOLUMN_NOT_EXIST() {
        return "列:[%s] 不存在.";
    }

    public String getCOLUMN_PARAMETER_INVALID(String column) {
        return String.format(Locale.ROOT, "列 [%s] 赋值失败，请检查该列类型后重新赋值。", column);
    }

    public String getINVALID_VOLATILE_RANGE_CONFIG() {
        return "重写模型设置失败，动态区间参数单位必须为天、周、月、年其中一个,且值必须为非负整数.";
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
    public String getJOB_NODE_INVALID(String url) {
        return "该服务请求无法在任务节点执行";
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
    public String getLICENSE_ERROR_PRE() {
        return "该许可证存在以下问题：\n";
    }

    @Override
    public String getLICENSE_ERROR_SUFF() {
        return "\n请上传新的许可证或联系 Kyligence 销售人员。";
    }

    @Override
    public String getUNSUPPORTED_RECOMMENDATION_MODE() {
        return "当前项目暂不支持模型推荐及优化，请在打开智能推荐开关后进行尝试。";
    }

    @Override
    public String getADD_JOB_CHECK_FAIL() {
        return "任务提交失败，相应构建对象下已有任务在运行。";
    }

    @Override
    public String getADD_JOB_EXCEPTION() {
        return "没有可执行的任务生成。";
    }

    @Override
    public String getADD_JOB_ABANDON() {
        return "添加job失败，该节点不是构建节点。";
    }

    @Override
    public String getADD_JOB_CHECK_SEGMENT_FAIL() {
        return "添加任务失败，segment的索引未对齐。";
    }

    @Override
    public String getEMPTY_DATABASE() {
        return "database 参数为空";
    }

    @Override
    public String getEMPTY_TABLE_LIST() {
        return "table 参数为空";
    }

    @Override
    public String getADD_JOB_CHECK_SEGMENT_READY_FAIL() {
        return "添加任务失败，没有ready状态的Segment。";
    }

    @Override
    public String getADD_JOB_CHECK_LAYOUT_FAIL() {
        return "添加任务失败，没有需要构建的layout。";
    }

    @Override
    public String getADD_JOB_CHECK_INDEX_FAIL() {
        return "添加任务失败，该segment的索引数是空.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_ABANDON() {
        return "添加任务失败，多级分区参数不合法。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_EMPTY() {
        return "添加任务失败，多级分区值为空。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_NOT_ALIGNED() {
        return "添加任务失败，多级分区没有对齐。";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_DUPLICATE(String dupPartitions) {
        return String.format(Locale.ROOT, "添加任务失败，分区 [%s] 是重复的.", dupPartitions);
    }

    @Override
    public String getTABLE_RELOAD_ADD_COLUMN_EXIST(String table, String column) {
        return String.format(Locale.ROOT, "当前暂不可重载表。表 %s 中已经存在列 %s。", table, column);
    }

    @Override
    public String getTABLE_RELOAD_HAVING_NOT_FINAL_JOB() {
        return "当前暂不可重载表。存在运行中的任务，任务对象为： %s。请等任务完成后再重载，或手动终止任务。";
    }

    @Override
    public String getCOLUMN_UNRECOGNIZED() {
        return "表达式中的列名无法识别 %s 。当引用列时，请使用 TABLE_ALIAS.COLUMN 格式来定义（TABLE_ALIAS 为模型中定义的表别名)。";
    }

    @Override
    public String getInvalidJobStatusTransaction() {
        return "无法 %s 状态为 %s 的任务 %s";
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
    public String getMODEL_ONLINE_WITH_EMPTY_SEG() {
        return "该模型尚未添加 Segment，不可服务于查询。请先添加 Segment 后再上线。";
    }

    @Override
    public String getMODEL_ONLINE_FORBIDDEN() {
        return "该模型不可上线";
    }

    // multi level partition mapping
    @Override
    public String getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID() {
        return "更新多级分区映射关系失败, 字段 'multi_partition_columns' 必须和模型中定义的多级分区列完全一致";
    }

    @Override
    public String getSCD2_MODEL_ONLINE_WITH_SCD2_CONFIG_OFF() {
        return "该模型因存在 ≥ 或 < 的连接条件，当前不可上线。请删除相应连接条件，或在项目设置中开启支持拉链表开关";
    }

    @Override
    public String getCONNECT_DATABASE_ERROR() {
        return "连接数据库异常。请检查数据库连接是否正常。";
    }

    // acl
    @Override
    public String getInvalidColumnAccess() {
        return "当前用户/组对 %s 列没有访问权限。";
    }

    @Override
    public String getInvalidSensitiveDataMaskColumnType() {
        return "Boolean、Map、Array 数据类型暂不支持脱敏。";
    }

    public String getNotSupportNestedDependentCol() {
        return "不支持在关联列 [%s] 上设置关联规则";
    }

    // Snapshots
    @Override
    public String getSNAPSHOT_OPERATION_PERMISSION_DENIED() {
        return "没有获取如下表信息的权限：'%s'。";
    }

    @Override
    public String getSNAPSHOT_NOT_FOUND() {
        return "获取不到如下快照：'%s'。";
    }

    @Override
    public String getSNAPSHOT_MANAGEMENT_NOT_ENABLED() {
        return "快照管理模式未开启";
    }

    @Override
    public String getINVALID_DIAG_TIME_PARAMETER() {
        return "终止时间必须大于起始时间，请输入正确的参数执行。";
    }

    // Resource Group
    @Override
    public String getRESOURCE_GROUP_FIELD_IS_NULL() {
        return "资源组请求字段均不能为 null。";
    }

    @Override
    public String getRESOURCE_CAN_NOT_BE_EMPTY() {
        return "在资源组模式下，至少存在一个资源组。";
    }

    @Override
    public String getEMPTY_RESOURCE_GROUP_ID() {
        return "资源组 id 不能为空。";
    }

    @Override
    public String getDUPLICATED_RESOURCE_GROUP_ID() {
        return "资源组不能有重复的 id。";
    }

    @Override
    public String getRESOURCE_GROUP_DISABLED_WITH_INVLIAD_PARAM() {
        return "关闭资源组前必须清空资源组信息.";
    }

    @Override
    public String getPROJECT_WITHOUT_RESOURCE_GROUP() {
        return "当前项目未绑定资源组，请联系管理员。";
    }

    @Override
    public String getEMPTY_KYLIN_INSTANCE_IDENTITY() {
        return "实例 instance 字段不能为空。";
    }

    @Override
    public String getEMPTY_KYLIN_INSTANCE_RESOURCE_GROUP_ID() {
        return "实例 resource_group_id 字段不能为空。";
    }

    @Override
    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_KYLIN_INSTANCE() {
        return "实例中的 resource_group_id 不存在。";
    }

    @Override
    public String getDUPLICATED_KYLIN_INSTANCE() {
        return "实例不能重复。";
    }

    @Override
    public String getEMPTY_PROJECT_IN_MAPPING_INFO() {
        return "mapping_info 中，project 不能为空。";
    }

    @Override
    public String getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO() {
        return "mapping_info 中，resource_group_id 不能为空。";
    }

    @Override
    public String getPROJECT_BINDING_RESOURCE_GROUP_INVALID() {
        return "一个项目最多绑定两个资源组，每种请求最多绑定一个资源组，不合理的项目名称： [%s].";
    }

    @Override
    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO() {
        return "mapping_info 中，resource_group_id 不存在。";
    }

    @Override
    public String getMODEL_IS_NOT_MLP() {
        return "模型 '%s' 没有子分区。";
    }

    @Override
    public String getPARTITION_VALUE_NOT_SUPPORT() {
        return "模型 '%s' 未设置子分区列，请先进行设置";
    }

    @Override
    public String getCONCURRENT_SUBMIT_JOB_LIMIT() {
        return "无法提交构建任务，单次最多可提交 %s 个构建任务。请尝试分批提交。";
    }

    @Override
    public String getADMIN_PERMISSION_UPDATE_ABANDON() {
        return "管理员不支持被更新权限.";
    }

    @Override
    public String getMODEL_ID_NOT_EXIST() {
        return "模型ID '%s' 不存在。";
    }

    @Override
    public String getNot_IN_EFFECTIVE_COLLECTION() {
        return "%s 不在有效的集合中 : %s。";
    }

    @Override
    public String getROW_ACL_NOT_STRING_TYPE() {
        return "like 行级权限仅支持 char 或 varchar 类型维度，请重新设置。";
    }
}
