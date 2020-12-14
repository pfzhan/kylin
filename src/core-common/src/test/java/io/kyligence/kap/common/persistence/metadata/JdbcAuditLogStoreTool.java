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
package io.kyligence.kap.common.persistence.metadata;

import java.util.UUID;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.util.AddressUtil;
import lombok.val;

public class JdbcAuditLogStoreTool {

    public static JdbcAuditLogStore prepareJdbcAuditLogStore(KylinConfig config) throws Exception {

        val url = config.getMetadataUrl();
        val auditLogStore = new JdbcAuditLogStore(config);

        val jdbcTemplate = auditLogStore.getJdbcTemplate();
        for (int i = 0; i < 100; i++) {
            val projectName = "p" + i;
            String unitId = UUID.randomUUID().toString();
            jdbcTemplate.update(String.format(JdbcAuditLogStore.INSERT_SQL, "test_audit_log"),
                    "/" + projectName + "/aa", "aa".getBytes(), System.currentTimeMillis(), 0, unitId, null,
                    AddressUtil.getLocalInstance());
        }

        return auditLogStore;
    }
}
