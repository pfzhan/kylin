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

package org.apache.kylin.rest.security;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExternalAclProviderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConvertToExternalPermissionWithMask() {
        String value = ExternalAclProvider.convertToExternalPermission(0);
        Assert.assertEquals("EMPTY", value);

        value = ExternalAclProvider.convertToExternalPermission(1);
        Assert.assertEquals("QUERY", value);

        value = ExternalAclProvider.convertToExternalPermission(64);
        Assert.assertEquals("OPERATION", value);

        value = ExternalAclProvider.convertToExternalPermission(32);
        Assert.assertEquals("MANAGEMENT", value);

        value = ExternalAclProvider.convertToExternalPermission(16);
        Assert.assertEquals("ADMIN", value);

        thrown.expectMessage("Invalid permission state: -1");
        ExternalAclProvider.convertToExternalPermission(-1);
    }

    @Test
    public void testConvertToExternalPermissionWithPermission() {
        String value = ExternalAclProvider.convertToExternalPermission(AclPermission.READ);
        Assert.assertEquals("QUERY", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.OPERATION);
        Assert.assertEquals("OPERATION", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.MANAGEMENT);
        Assert.assertEquals("MANAGEMENT", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.ADMINISTRATION);
        Assert.assertEquals("ADMIN", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.CREATE);
        Assert.assertEquals(".............................C..", value);
    }

    @Test
    public void testCheckExternalPermission() {
        ExternalAclProvider.checkExternalPermission("ADMIN");
        ExternalAclProvider.checkExternalPermission("MANAGEMENT");
        ExternalAclProvider.checkExternalPermission("OPERATION");
        ExternalAclProvider.checkExternalPermission("QUERY");
    }

    @Test
    public void testCheckExternalPermissionWithEmptyPermission() {
        thrown.expectMessage("Permission should not be empty.");
        ExternalAclProvider.checkExternalPermission("");
    }

    @Test
    public void testCheckExternalPermissionWithInvalidPermission() {
        ExternalAclProvider.checkExternalPermission("ADMIN");
        thrown.expectMessage("Invalid values in parameter \"permission\". "
                + "The value should either be \"ADMIN\", \"MANAGEMENT\", \"OPERATION\" or \"QUERY\".");
        ExternalAclProvider.checkExternalPermission("TEST");
    }
}