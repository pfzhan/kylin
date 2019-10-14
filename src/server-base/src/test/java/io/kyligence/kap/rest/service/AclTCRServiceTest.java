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

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.util.Set;

public class AclTCRServiceTest {

    @InjectMocks
    private AclTCRService aclTCRService = Mockito.spy(new AclTCRService());

    @Test
    public void testGetAuthorizedTables() {
        String userName = "user1";
        Set<String> groups = Sets.newHashSet("group1");

        NKylinUserManager nKylinUserManager = Mockito.mock(NKylinUserManager.class);
        Mockito.doReturn(nKylinUserManager).when(aclTCRService).getKylinUserManager();

        Mockito.doReturn(groups).when(nKylinUserManager).getUserGroups(userName);
        Mockito.doReturn(null).when(aclTCRService).getAuthorizedTables("default", userName, Sets.newHashSet("group1"));

        Assert.assertNull(aclTCRService.getAuthorizedTables("default", userName));

        Mockito.reset(aclTCRService);
        AclTCRManager aclTCRManager = Mockito.mock(AclTCRManager.class);
        Mockito.doReturn(aclTCRManager).when(aclTCRService).getAclTCRManager("default");
        Mockito.doReturn(Lists.newArrayList()).when(aclTCRManager).getAclTCRs(userName, groups);

        NTableMetadataManager nTableMetadataManager = Mockito.mock(NTableMetadataManager.class);
        Mockito.doReturn(nTableMetadataManager).when(aclTCRService).getTableMetadataManager("default");
        Mockito.doReturn(Lists.newArrayList()).when(nTableMetadataManager).listAllTables();
        Mockito.doReturn(false).when(aclTCRService).canUseACLGreenChannel("default");

        Assert.assertEquals(0, aclTCRService.getAuthorizedTables("default", userName, groups).size());
    }

}
