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

package org.apache.kylin.rest.msg;

import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;

public class CnMessage extends Message {

    private static CnMessage instance = null;

    protected CnMessage() {

    }

    public static CnMessage getInstance() {
        if (instance == null) {
            instance = new CnMessage();
        }
        return instance;
    }

    // Cube
    public String getCHECK_CC_AMBIGUITY() {
        return "当前模型下，可计算列名[%s]已被使用，请重新命名可计算列名";
    }

    public String getCUBE_NOT_FOUND() {
        return "找不到 Cube '%s'";
    }

    public String getSEG_NOT_FOUND() {
        return "找不到 Segment '%s' 在模型 '%s' 上";
    }

    public String getKAFKA_DEP_NOT_FOUND() {
        return "找不到 Kafka 依赖";
    }

    public String getBUILD_DRAFT_CUBE() {
        return "Cube 草稿不能被构建";
    }

    public String getBUILD_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被构建";
    }

    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    }

    public String getDELETE_NOT_FIRST_LAST_SEG() {
        return "非首尾 segment '%s' 不能被删除";
    }

    public String getDELETE_NOT_READY_SEG() {
        return "非 READY 状态 segment '%s' 不能被删除, 请先抛弃它正在运行的任务";
    }

    public String getINVALID_BUILD_TYPE() {
        return "非法构建类型: '%s'";
    }

    public String getNO_ACL_ENTRY() {
        return "找不到对象 '%s' 的授权记录";
    }

    public String getACL_INFO_NOT_FOUND() {
        return "找不到对象 '%s' 的授权信息";
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return "找不到授权对象";
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return "找不到上级授权";
    }

    public String getDISABLE_NOT_READY_CUBE() {
        return "仅 ready 状态的 cube 可以被禁用, '%s' 的状态是 %s";
    }

    public String getPURGE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被清空, '%s' 的状态是 %s";
    }

    public String getCLONE_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被克隆";
    }

    public String getINVALID_CUBE_NAME() {
        return "非法 cube 名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getCUBE_ALREADY_EXIST() {
        return "Cube 名称 '%s' 已存在";
    }

    public String getCUBE_DESC_ALREADY_EXIST() {
        return "Cube '%s' 已存在";
    }

    public String getBROKEN_CUBE_DESC() {
        return "损坏的 Cube 描述 '%s'";
    }

    public String getENABLE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被启用, '%s' 的状态是 %s";
    }

    public String getNO_READY_SEGMENT() {
        return "Cube '%s' 不包含任何 READY 状态的 segment";
    }

    public String getENABLE_WITH_RUNNING_JOB() {
        return "Cube 存在正在运行的任务, 不能被启用";
    }

    public String getDISCARD_JOB_FIRST() {
        return "Cube '%s' 存在正在运行或失败的任务, 请抛弃它们后重试";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "'%s' 存在下级授权";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "非法 cube 定义";
    }

    public String getEMPTY_CUBE_NAME() {
        return "Cube 名称不可为空";
    }

    public String getUSE_DRAFT_MODEL() {
        return "不能使用模型草稿 '%s'";
    }

    public String getINCONSISTENT_CUBE_DESC() {
        return "Cube 描述 '%s' 与现有不一致， 请清理 cube 或避免更新 cube 描述的关键字段";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "无权限更新此 cube";
    }

    public String getNOT_STREAMING_CUBE() {
        return "Cube '%s' 不是实时 cube";
    }

    public String getCUBE_RENAME() {
        return "Cube 不能被重命名";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "非法模型定义";
    }

    public String getEMPTY_MODEL_NAME() {
        return "模型名称不可为空";
    }

    public String getINVALID_MODEL_NAME() {
        return "非法模型名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getINVALID_DIMENSION_NAME() {
        return "非法维度名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getINVALID_MEASURE_NAME() {
        return "非法度量名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getMODEL_ID_NOT_FOUND() {
        return "模型 ID 不能为空";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "模型名称 '%s' 已存在, 不能被创建";
    }

    public String getDROP_REFERENCED_MODEL() {
        return "模型被 Cube '%s' 引用, 不能被删除";
    }

    public String getUPDATE_MODEL_KEY_FIELD() {
        return "由于维度、度量或者连接关系被修改导致与存在的cube定义不一致，因而当前模型无法保存。";
    }

    public String getBROKEN_MODEL_DESC() {
        return "损坏的模型描述 '%s'";
    }

    public String getMODEL_NOT_FOUND() {
        return "找不到模型 '%s'";
    }

    public String getINDEX_ALREADY_DELETED() {
        return "该索引已经被删除";
    }

    public String getEMPTY_PROJECT_NAME() {
        return "没有项目信息，请指定一个项目。";
    }

    public String getPROJECT_UNMODIFIABLE_REASON() {
        return "当前项目暂不支持模型推荐及优化，请在打开智能推荐开关后进行尝试。";
    }

    public String getPROJECT_MODE_CHANGE_ERROR() {
        return "当前正在进行优化建议转化，该过程可能持续数分钟，请稍后再关闭智能推荐";
    }

    public String getEMPTY_NEW_MODEL_NAME() {
        return "新模型名称不可为空";
    }

    public String getUPDATE_MODEL_NO_RIGHT() {
        return "无权限更新此模型";
    }

    public String getMODEL_RENAME() {
        return "模型不能被重命名";
    }

    public String getDUPLICATE_DIMENSION_NAME() {
        return "维度名称 '%s' 已存在";
    }

    public String getDUPLICATE_MEASURE_NAME() {
        return "度量名称 '%s' 已存在";
    }

    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "维度表达 '%s' 已存在，不能被创建";
    }

    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "Join条件 '%s'和'%s' 已存在，不能被创建";
    }

    public String getCheckCCType() {
        return "可计算列 {0} 定义的数据类型 {2} 与实际类型{1} 不符，请修改后进行重试";
    }

    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "解析失败，请检查模型数据包是否完整。";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_ALREADY_DEFINED() {
        return "模型 '%s' 导入失败：该模型的可计算列 '%s' 与模型 '%s' 中的可计算列 '%s' 表达式相同。";
    }

    public String getCOMPUTED_COLUMN_NAME_ALREADY_DEFINED() {
        return "模型 '%s' 导入失败：该模型的可计算列 '%s' 与模型 '%s' 中的可计算列重名。";
    }

    public String getFACT_TABLE_USED_AS_LOOK_UP_TABLE() {
        return "模型 '%s' 导入失败：模型中的事实表在其他模型中已经作为维度表存在。";
    }

    // Job
    public String getILLEGAL_TIME_FILTER() {
        return "非法时间条件: %s";
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return "非法状态: %s";
    }

    public String getILLEGAL_JOB_TYPE() {
        return "非法任务类型, id: %s.";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "用户 '%s' 不存在, 请确认用户是否存在。";
    }

    // Project
    public String getINVALID_PROJECT_NAME() {
        return "项目名称只支持数字、字母和下划线，并且需要用数字或者字母开头。";
    }

    public String getPROJECT_ALREADY_EXIST() {
        return "项目 '%s' 已存在";
    }

    public String getPROJECT_NOT_FOUND() {
        return "找不到项目 '%s'";
    }

    public String getDELETE_PROJECT_NOT_EMPTY() {
        return "不能修改该项目，如需要修改请先清空其中的Cube和Model";
    }

    public String getRENAME_PROJECT_NOT_EMPTY() {
        return "不能重命名该项目，如果要重命名请先清空其中的Cube和Model";
    }

    // Table
    public String getHIVE_TABLE_NOT_FOUND() {
        return "找不到 Hive 表 '%s'";
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return "找不到表 '%s'";
    }

    public String getTABLE_IN_USE_BY_MODEL() {
        return "表已被模型 '%s' 使用";
    }

    // table sampling
    public String getBEYOND_MIX_SAMPLING_ROWSHINT() {
        return "采样的行数低于了最小采样行数（ %d 行）";
    }

    public String getBEYOND_MAX_SAMPLING_ROWS_HINT() {
        return "采样的行数超过了最大采样行数（ %d 行）";
    }

    public String getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME() {
        return "采样表的名称不符合规范，表名正确格式：database.table";
    }

    public String getFAILED_FOR_IN_SAMPLING() {
        return "表 %s 有正在进行的抽样任务，暂时无法触发新一个抽样任务。";
    }

    public String getFAILED_FOR_NO_SAMPLING_TABLE() {
        return "没有传入采样表名称（database.table），请至少提供一张表";
    }

    // Cube Desc
    public String getCUBE_DESC_NOT_FOUND() {
        return "找不到 cube '%s'";
    }

    // Streaming
    public String getINVALID_TABLE_DESC_DEFINITION() {
        return "非法表定义";
    }

    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return "非法 StreamingConfig 定义";
    }

    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "非法 KafkaConfig 定义";
    }

    public String getADD_STREAMING_TABLE_FAIL() {
        return "添加流式表失败";
    }

    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return "StreamingConfig 名称不可为空";
    }

    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return "StreamingConfig '%s' 已存在";
    }

    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return "保存 StreamingConfig 失败";
    }

    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return "KafkaConfig '%s' 已存在";
    }

    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return "StreamingConfig 已创建, 但 KafkaConfig 创建失败";
    }

    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return "KafkaConfig 保存失败";
    }

    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 StreamingConfig 失败";
    }

    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 KafkaConfig 失败";
    }

    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return "无权限更新此 StreamingConfig";
    }

    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return "无权限更新此 KafkaConfig";
    }

    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return "找不到 StreamingConfig '%s'";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "'%s' 模式不支持查询";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "不支持的 SQL";
    }

    public String getTABLE_META_INCONSISTENT() {
        return "表元数据与JDBC 元数据不一致";
    }

    public String getCOLUMN_META_INCONSISTENT() {
        return "列元数据与JDBC 元数据不一致";
    }

    public String getDUPLICATE_QUERY_NAME() {
        return "查询名称重复 '%s'";
    }

    public String getNULL_EMPTY_SQL() {
        return "SQL不能为空";
    }

    // Access
    public String getACL_PERMISSION_REQUIRED() {
        return "需要授权";
    }

    public String getSID_REQUIRED() {
        return "找不到 Sid";
    }

    // user group

    public String getEMPTY_GROUP_NAME() {
        return "用户组名不能为空.";
    }

    public String getEMPTY_SID() {
        return "用户名/用户组名不能为空";
    }

    public String getINVALID_SID() {
        return "用户名/组名只能包含字母，数字，空格和下划线";
    }

    public String getEMPTY_QUERY_NAME() {
        return "查询名称不能为空";
    }

    public String getINVALID_QUERY_NAME() {
        return "查询名称只能包含字母，数字和下划线";
    }

    public String getREVOKE_ADMIN_PERMISSION() {
        return "不能取消创建者的管理员权限";
    }

    public String getACE_ID_REQUIRED() {
        return "找不到 Ace id";
    }

    public String getGroup_EDIT_NOT_ALLOWED() {
        return "暂不支持LDAP认证机制下的用户组编辑操作";
    }

    public String getGroup_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "暂不支持客户认证接入机制下的用户组编辑操作, 方法 '%s' 未被实现";
    }

    // Async Query
    public String getQUERY_RESULT_NOT_FOUND() {
        return "该查询不存在有效的结果, 请首先检查它的状态";
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "查询结果文件不存在";
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "查询异常文件不存在";
    }

    public String getCLEAN_FOLDER_FAIL() {
        return "清理文件夹失败";
    }

    // Admin
    public String getGET_ENV_CONFIG_FAIL() {
        return "无法获取 Kylin env Config";
    }

    // User
    public String getAUTH_INFO_NOT_FOUND() {
        return "找不到权限信息";
    }

    public String getUSER_NOT_FOUND() {
        return "找不到用户 '%s'";
    }

    public String getUSER_BE_LOCKED() {
        return "用户名或密码错误，请在30秒后再次重试";
    }

    public String getUSER_IN_LOCKED_STATUS() {
        return "用户 %s 已被锁定, 请在 %s 秒后重试";
    }

    public String getUSER_AUTH_FAILED() {
        return "用户名或密码错误。";
    }

    public String getUSER_LOGIN_AS_USER_NOT_ADMIN() {
        return "仅 ADMIN 用户可切换为其他用户登录。";
    }

    public String getNEW_PASSWORD_SAME_AS_OLD() {
        return "新密码与旧密码一致，请输入一个不同的新密码";
    }

    public String getUSER_EDIT_NOT_ALLOWED() {
        return "暂不支持LDAP认证机制下的用户编辑操作";
    }

    public String getUSER_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "暂不支持客户认证接入机制下的用户编辑操作, 方法 '%s' 未被实现";
    }

    // Diagnosis
    public String getDIAG_NOT_FOUND() {
        return "在 %s 找不到 diag.sh";
    }

    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return "无法生成诊断包";
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "诊断包不可用, 路径: %s";
    }

    public String getDIAG_PACKAGE_NOT_FOUND() {
        return "找不到诊断包, 路径: %s";
    }

    // Encoding
    public String getVALID_ENCODING_NOT_AVAILABLE() {
        return "无法为数据类型: %s 提供合法的编码";
    }

    // ExternalFilter
    public String getFILTER_ALREADY_EXIST() {
        return "Filter '%s' 已存在";
    }

    public String getFILTER_NOT_FOUND() {
        return "找不到 filter '%s'";
    }

    // Basic
    public String getHBASE_FAIL() {
        return "HBase 遇到错误: '%s'";
    }

    public String getHBASE_FAIL_WITHOUT_DETAIL() {
        return "HBase 遇到错误";
    }

    // Favorite Query
    public String getFAVORITE_RULE_NOT_FOUND() {
        return "无法找到相应的加速规则 '%s' ";
    }

    public String getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH() {
        return "目前待加速的加速查询未达到 '%s' 条";
    }

    public String getFAIL_TO_VERIFY_SQL() {
        return "验证sql失败.";
    }

    public String getFAVORITE_QUERY_NOT_EXIST() {
        return "加速查询 '%s' 不存在";
    }

    // Query statistics

    public String getNOT_SET_INFLUXDB() {
        return "未设置参数 kap.metric.write-destination 为 INFLUX";
    }

    //license

    public String getLICENSE_OVERDUE_TRIAL() {
        return "许可证已过期，当前有效期为[%s - %s]。请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    public String getLICENSE_NODES_EXCEED() {
        return "您使用的节点数已超过许可证范围，请联系您的客户经理。";
    }

    public String getLICENSE_WRONG_CATEGORY() {
        return "当前许可证的版本与产品不匹配，请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    public String getLICENSE_NO_LICENSE() {
        return "没有许可证文件。请联系 Kyligence 销售人员。";
    }

    public String getLICENSE_INVALID_LICENSE() {
        return "无效许可证，请联系 Kyligence 销售人员。";
    }

    public String getLICENSE_MISMATCH_LICENSE() {
        return "许可证适用的集群信息与当前不符，请重新上传新的许可证或联系 Kyligence 销售人员。";
    }

    public String getLICENSE_NOT_EFFECTIVE() {
        return "许可证尚未生效，请重新申请。";
    }

    public String getLICENSE_EXPIRED() {
        return "许可证已过期，当前有效期为 [%s - %s]。请联系 Kyligence 销售人员。";
    }

    // ICredential

    public String getINVALID_CREDENTIAL() {
        return "错误凭证,请检查您的凭证";
    }

    public String getINVALID_URL() {
        return "错误文件地址，请检查您的文件地址";
    }

    public String getNoTableFound() {
        return "找不到表。请检查您的SQL语句";
    }

    public String getNoJobNode() {
        return "未发现执行任务的节点。请启动对应节点或配置一个任务节点（ 配置文件${KYLIN_HOME}/conf/kylin.properties，配置项 kylin.server.mode）。";
    }

    public String getTABLENOTFOUND() {
        return "模型 [%s] 保存失败，请确保模型中使用的列 [%s] 在源表 [%s] 中存在";
    }

    // Async push down get data range
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "获取失败，请手动选择数据范围";
    }

    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "获取超时，请手动选择数据范围";
    }

    public String getCHANGE_GLOBALADMIN() {
        return "您不可以添加，修改，删除系统管理员的权限。";
    }

    public String getCHANGE_DEGAULTADMIN() {
        return "由于用户ADMIN是系统默认内置管理员，所以不可以移除ROLE_ADMIN权限，删除，禁用用户ADMIN，并且仅用户ADMIN才可以更改用户ADMIN的密码和用户组";
    }

    //Query
    public String getINVALID_USER_TAG() {
        return "user_defined_tag 必须小于等于 256.";
    }

    public String getINVALID_ID() {
        return "id {%s} 不存在";
    }

    public String getSEGMENT_LOCKED() {
        return "由于 Segment [%s]被锁定，所以不能删除或者刷新该 Segment。";
    }

    public String getSEGMENT_STATUS(SegmentStatusEnumToDisplay status) {
        return "由于 Segment [%s] 处于 " + status + " 状态，所以不能合并或者刷新该 Segment。";
    }

    //Kerberos
    public String getKERBEROS_INFO_ERROR() {
        return "无效的 Principle 名称或者 Keytab 文件，请检查后重试.";
    }

    public String getPROJECT_HIVE_PERMISSION_ERROR() {
        return "权限不足，请确保提交的 Kerberos 用户信息包含所有已加载表的访问权限.";
    }

    //HA
    public String getNO_ACTIVE_LEADERS() {
        return "系统中暂无活跃的任务节点，请联系您的系统管理员进行检查并修复。";
    }

    public String getLEADERS_HANDLE_OVER() {
        return "系统正在尝试恢复服务，请稍后进行尝试。";
    }

    public String getTABLE_REFRESH_NOTFOUND() {
        return "未找到缓存表: %s";
    }

    public String getTABLE_REFRESH_ERROR() {
        return "刷新缓存表异常";
    }

    public String getTABLE_REFRESH_PARAM_INVALID() {
        return "请求中tables字段非法，请检查后重试.";
    }

    public String getTABLE_REFRESH_PARAM_NONE() {
        return "请求中未发现tables字段，请检查后重试.";
    }

    public String getTABLE_REFRESH_PARAM_MORE() {
        return "请求中包含非tables的多余字段，请检查后重试.";
    }

    public String getTRANSFER_FAILED() {
        return "转发失败。";
    }

    public String getUSER_EXISTS() {
        return "用户名:[%s] 已存在。";
    }

    public String getOPERATION_FAILED_BY_USER_NOT_EXIST() {
        return "操作失败，用户[%s]不存在，请先添加";
    }

    public String getOPERATION_FAILED_BY_GROUP_NOT_EXIST() {
        return "操作失败，用户组[%s]不存在，请先添加";
    }

    public String getPERMISSION_DENIED() {
        return "拒绝访问";
    }

    public String getCOLUMU_IS_NOT_DIMENSION() {
        return "列[%s]不是一个维度";
    }

    public String getMODEL_CAN_NOT_PURGE() {
        return "模型[%s]无法被清除";
    }

    public String getMODEL_SEGMENT_CAN_NOT_REMOVE() {
        return "模型[%s]不能手动清除Segment";
    }

    public String getSEGMENT_CAN_NOT_REFRESH() {
        return "由于您需要刷新的Segment中有部分Segment正在构建，您现在无法刷新。";
    }

    public String getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE() {
        return "可用的Segment范围已更改，无法刷新，请重试。";
    }

    public String getCAN_NOT_BUILD_SEGMENT() {
        return "无法构建Segment，请先定义聚合索引或者明细索引。";
    }

    public String getCAN_NOT_BUILD_SEGMENT_MANUALLY() {
        return "模型[%s]不能手动构建Segment";
    }

    public String getCAN_NOT_BUILD_INDICES_MANUALLY() {
        return "模型[%s]不能手动构建索引";
    }

    public String getINVALID_REMOVE_SEGMENT() {
        return "只有头部或尾部的连续的Segment才能被移除！";
    }

    public String getINVALID_MERGE_SEGMENT() {
        return "无法合并暂不可用的Segment";
    }

    public String getINVALID_SET_TABLE_INC_LOADING() {
        return "无法设置表[％s]的增量加载，因为另一个模型[％s]将该表用作维表";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT() {
        return "没有可用的Segment可以刷新";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NOT_READY() {
        return "刷新范围内的数据必须就绪";
    }

    public String getINVALID_LOAD_HIVE_TABLE_NAME() {
        return "不能执行该操作，请设置kap.table.load-hive-tablename-cached.enabled=true，然后重试";
    }

    public String getINVALID_REMOVE_USER_FROM_ALL_USER() {
        return "无法从ALL USERS组中删除用户。";
    }

    public String getACCESS_DENY_ONLY_ADMIN() {
        return "拒绝访问，只有系统和项目管理员才能编辑用户的表，列和行权限";
    }

    public String getACCESS_DENY_ONLY_ADMIN_AND_PROJECT_ADMIN() {
        return "拒绝访问，只有系统管理员才能编辑用户的表，列和行权限";
    }

    public String getQUERY_TOO_MANY_RUNNING() {
        return "并发查询请求太多。";
    }

    public String getSELF_DISABLE_FORBIDDEN() {
        return "您不可以禁用您自己";
    }

    public String getSELF_DELETE_FORBIDDEN() {
        return "您不可以删除您自己";
    }

    public String getOLD_PASSWORD_WRONG() {
        return "原密码不正确";
    }

    public String getINVALID_PASSWORD() {
        return "密码应至少包含一个数字，字母和特殊字符（〜！@＃$％^＆*（）{} |：\\“ <>？[]; \\'\\，。/`）。";
    }

    public String getSHORT_PASSWORD() {
        return "密码应包含8个以上的字符！";
    }

    public String getSEGMENT_LIST_IS_EMPTY() {
        return "Segments 列表为空";
    }

    public String getINVALID_REFRESH_SEGMENT() {
        return "您应该至少选择一个Segment来刷新。";
    }

    public String getINVALID_MERGE_SEGMENT_BY_TOO_LESS() {
        return "您应该至少选择两个Segment来合并。";
    }

    public String getCONTENT_IS_EMPTY() {
        return "许可证内容为空";
    }

    public String getINLEGAL_EMAIL() {
        return "不允许使用个人电子邮件或非法电子邮件";
    }

    public String getLICENSE_ERROR() {
        return "获取许可证失败";
    }

    public String getEMAIL_USERNAME_COMPANY_CAN_NOT_EMPTY() {
        return "邮箱, 用户名, 公司不能为空";
    }

    public String getEMAIL_USERNAME_COMPANY_IS_ILLEGAL() {
        return "邮箱, 用户名, 公司的长度要小于等于50";
    }

    public String getINVALID_COMPUTER_COLUMN_NAME() {
        return "计算列[%s]的名称是 SQL 关键字，请使用其他名称。";
    }

    public String getMODEL_ALIAS_DUPLICATED() {
        return "模型表名[%s]已存在";
    }

    public String getINVALID_RANGE_LESS_THAN_ZERO() {
        return "起始时间和终止时间必须大于0";
    }

    public String getINVALID_RANGE_NOT_FORMAT() {
        return "无效的起始时间或终止时间格式。仅支持时间戳类型，单位毫秒";
    }

    public String getINVALID_RANGE_END_LESSTHAN_START() {
        return "终止时间必须大于起始时间";
    }

    public String getINVALID_RANGE_NOT_CONSISTENT() {
        return "起始时间和终止时间必须同时存在或者同时不存在";
    }

    public String getID_CANNOT_EMPTY() {
        return "ID不能为空";
    }

    public String getINVALID_CREATE_MODEL() {
        return "无法在SQL加速项目中手动创建模型！";
    }

    public String getSEGMENT_INVALID_RANGE() {
        return "要刷新的[%s] Segment 范围已经超出了加载数据的范围，加载数据的范围是[%s]";
    }

    public String getSEGMENT_RANGE_OVERLAP() {
        return "将要构建的范围和已构建的范围重合，从[%s]到[%s]，请选择新的数据范围，然后重试";
    }

    public String getPARTITION_COLUMN_NOT_EXIST() {
        return "分区列不存在";
    }

    public String getINVALID_PARTITION_COLUMN() {
        return "时间分区列必须使用事实表上的原始列";
    }

    public String getTABLE_NAME_CANNOT_EMPTY() {
        return "必须指定表名！";
    }

    public String getTABLE_NOT_FOUND() {
        return "找不到表[%s]";
    }

    public String getILLEGAL_JOB_STATE() {
        return "非法的任务状态:%s";
    }

    public String getFILE_NOT_EXIST() {
        return "找不到文件[%s]";
    }

    public String getDATABASE_NOT_EXIST() {
        return "数据库:[%s]不存在";
    }

    public String getBROKEN_MODEL_CANNOT_ONOFFLINE() {
        return "处于Broken状态的模型[%s]无法上线或下线";
    }

    public String getINVALID_NAME_START_WITH_DOT() {
        return "用户名/用户组名不能以英文句号开头(.)";
    }

    public String getINVALID_NAME_START_OR_END_WITH_BLANK() {
        return "用户名/用户组名不能以空格开头或结尾";
    }

    public String getINVALID_NAME_CONTAINS_OTHER_CHARACTER() {
        return "用户名/用户组中仅支持英文字符";
    }

    public String getINVALID_NAME_CONTAINS_INLEGAL_CHARACTER() {
        return "用户名/用户组名中不能包含如下符号: 反斜杠(\\), 斜杠(/), 冒号(:), 星号(*), 问号(?), 引号(“), 小于号(<), 大于号(>), 垂直线(|)";
    }

    public String getHIVETABLE_NOT_FOUND() {
        return "数据源中以下表加载失败：{%s}。请检查数据源。";
    }

}
