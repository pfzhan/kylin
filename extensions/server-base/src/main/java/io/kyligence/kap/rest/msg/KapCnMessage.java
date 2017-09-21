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

package io.kyligence.kap.rest.msg;

/**
 * Created by luwei on 17-5-10.
 */
public class KapCnMessage extends KapMessage {

    private static KapCnMessage instance = null;

    protected KapCnMessage() {

    }

    public static KapCnMessage getInstance() {
        if (instance == null) {
            instance = new KapCnMessage();
        }
        return instance;
    }

    // KAP Async Query
    public String getCLEAN_FOLDER_FAIL() {
        return "清理文件夹失败";
    }

    public String getQUERY_EXCEPTION_NOT_FOUND() {
        return "该查询不存在有效的异常, 请首先检查它的状态";
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "查询异常文件不存在";
    }

    public String getQUERY_RESULT_NOT_FOUND() {
        return "该查询不存在有效的结果, 请首先检查它的状态";
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "查询结果文件不存在";
    }

    // Cube
    public String getCUBE_NOT_FOUND() {
        return "找不到 Cube '%s'";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "无权限更新此 cube";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "非法 cube 定义";
    }

    // KAP Cube
    public String getRAW_SEG_SIZE_NOT_ONE() {
        return "至少应存在一个 rawtable segment";
    }

    // KAP Table
    public String getHIVE_TABLE_LOAD_FAILED() {
        return "加载 Hive 数据库/表信息失败. 请重新点击 \"Hive Tables\" 按键重新加载";
    }

    // KAP User
    public String getEMPTY_USER_NAME() {
        return "用户名不可为空";
    }

    public String getSHORT_PASSWORD() {
        return "密码应包含至少8个字符!";
    }

    public String getINVALID_PASSWORD() {
        return "密码应包含至少一个数字, 字母和特殊字符 (~!@#$%^&*(){}|:\"<>?[];\\'\\,./`)";
    }

    public String getPERMISSION_DENIED() {
        return "没有权限!";
    }

    public String getSELF_DELETE_FORBIDDEN() {
        return "无法删除自身账户！";
    }

    public String getSELF_DISABLE_FORBIDDEN() {
        return "无法禁用自身账户";
    }

    public String getUSER_EDIT_NOT_ALLOWED() {
        return "只有testing profile才允许编辑用户，请在LDAP/SAML管理软件中设置";
    }

    public String getOLD_PASSWORD_WRONG() {
        return "原密码不正确!";
    }

    // KAP Raw Table
    public String getINVALID_RAWTABLE_DEFINITION() {
        return "非法 Rawtable 定义";
    }

    public String getEMPTY_RAWTABLE_NAME() {
        return "RawTable 名称不可为空";
    }

    public String getRAWTABLE_ALREADY_EXIST() {
        return "RawTable '%s' 已存在";
    }

    public String getRAWTABLE_NOT_FOUND() {
        return "RawTable '%s' 不存在";
    }

    public String getRAW_DESC_RENAME() {
        return "RawTable 不能被重命名: 新名称: '%s', 原名称: '%s'.";
    }

    public String getRAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB() {
        return "RawTable 存在正在运行的任务, 不能被修改";
    }

    public String getRAWTABLE_HAS_RUNNING_JOB() {
        return "RawTable 存在正在运行的任务, 请抛弃它们后重试";
    }

    public String getENABLE_NOT_DISABLED_RAWTABLE() {
        return "仅 disabled 状态的 RawTable 可以被启用, '%s' 的状态是 %s";
    }

    public String getRAWTABLE_NO_READY_SEGMENT() {
        return "RawTable '%s' 不包含任何 READY 状态的 segment";
    }

    public String getRAWTABLE_ENABLE_WITH_RUNNING_JOB() {
        return "存在正在运行的任务, 不能被启用";
    }

    public String getDISABLE_NOT_READY_RAWTABLE() {
        return "仅 ready 状态的 rawTable 可以被禁用, '%s' 的状态是 %s";
    }

    public String getRAWTABLE_DESC_NOT_FOUND() {
        return "RawTable '%s' 不存在";
    }

    public String getRAWTABLE_RENAME() {
        return "RawTable 不能被重命名";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "'%s' 模式不支持查询";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "不支持的 SQL";
    }

    // KAP Config
    public String getEMPTY_FEATURE_NAME() {
        return "请求字段 feature_name 不可为空";
    }

    // KAP KyBot
    public String getDUMP_KYBOT_PACKAGE_FAIL() {
        return "生成KyBot诊断包失败";
    }

    public String getKYACCOUNT_AUTH_FAILURE() {
        return "登录失败，请检查您的用户名和密码。";
    }

    public String getKYBOT_NOACCESS() {
        return "无法连接到Internet, 请检查网络连接。\n如需设置代理，可在kylin.properties通过kap.external.http.proxy.host和kap.external.http.proxy.port配置主机名和端口号";
    }

    // KAP Metastore
    public String getKYLIN_HOME_UNDEFINED() {
        return "KYLIN_HOME 未定义";
    }

    // streaming
    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "非法 KafkaConfig 定义";
    }

    // KAP system
    public String getDOWNLOAD_FILE_CREATE_FAIL() {
        return "创建下载文件失败";
    }

    // KAP Table Ext
    public String getJOB_INSTANCE_NOT_FOUND() {
        return "找不到任务";
    }

    // KAP Authentication
    public String getUSER_LOCK() {
        return "用户 %s 已被锁定, 请等待 %s 秒";
    }

    public String getUSER_AUTHFAILED() {
        return "登陆认证信息错误，请确认您的用户和密码正确。";
    }

    public String getSQL_VALIDATE_FAILED() {
        return "sql校验错误";
    }
}
