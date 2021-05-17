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

package io.kyligence.kap.clickhouse.management;

import lombok.Data;

@Data
public class RecoveryParameter {
    public static final String CLICKHOUSE_DEFAULT_DATA_PATH = "/var/lib/clickhouse";

    private String sourceIp;
    private int sourceSshPort = 22;
    private String sourceSshUser;
    private String sourceSshPassword;
    private int sourceCHPort = 9000;
    private String sourceCHUser = "default";
    private String sourceCHPassword = "";
    private String targetIp;
    private int targetSshPort = 22;
    private String targetSshUser;
    private String targetSshPassword;
    private int targetCHPort = 9000;
    private String targetCHUser = "default";
    private String targetCHPassword = "";
    private String database;
    private String table;
    private String sourceDataPath = CLICKHOUSE_DEFAULT_DATA_PATH;
    private String targetDataPath = CLICKHOUSE_DEFAULT_DATA_PATH;

    public String getSourceJdbcUrl() {
        return getJdbcUrl(sourceIp, sourceCHPort);
    }

    public String getTargetJdbcUrl() {
        return getJdbcUrl(targetIp, targetCHPort);
    }

    private String getJdbcUrl(String ip, int port) {
        return "jdbc:clickhouse://" + ip + ":" + port;
    }
}
