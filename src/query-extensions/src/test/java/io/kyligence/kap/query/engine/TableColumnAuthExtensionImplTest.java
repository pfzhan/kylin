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

package io.kyligence.kap.query.engine;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.query.QueryExtension;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

@MetadataInfo(onlyProps = true)
@OverwriteProp(key = "kylin.query.security.acl-tcr-enabled", value = "true")
class TableColumnAuthExtensionImplTest {
    private final String projectDefault = "default";
    private final String user1 = "u1";
    private final String group1 = "g1";
    private final String group2 = "g2";

    @Test
    void testIsColumnsAuthorized() {
        TableColumnAuthExtension extension = QueryExtension.getFactory().getTableColumnAuthExtension();

        Set<String> groups = new HashSet<>();
        groups.add(group1);
        groups.add(group2);

        Set<String> columns = new HashSet<>();
        columns.add("DEFAULT.TEST_COUNTRY.COUNTRY");
        Assert.assertFalse(extension.isColumnsAuthorized(getTestConfig(),
                projectDefault, user1, groups, columns));

        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        columns.add("DEFAULT.TEST_KYLIN_FACT.TRANS_ID");
        Assert.assertTrue(extension.isColumnsAuthorized(getTestConfig(),
                projectDefault, user1, groups, columns));
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }
}
