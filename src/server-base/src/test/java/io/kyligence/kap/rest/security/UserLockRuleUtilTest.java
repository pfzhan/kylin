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

package io.kyligence.kap.rest.security;

import io.kyligence.kap.metadata.user.ManagedUser;
import org.junit.Assert;
import org.junit.Test;

public class UserLockRuleUtilTest {

    @Test
    public void testIsLockedPermanently() {
        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");
        managedUser.setWrongTime(9);

        Assert.assertFalse(UserLockRuleUtil.isLockedPermanently(managedUser));

        managedUser.setWrongTime(10);
        Assert.assertTrue(UserLockRuleUtil.isLockedPermanently(managedUser));
    }

    @Test
    public void testIsLockDurationEnded() {
        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");

        managedUser.setWrongTime(3);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 30 * 1000L));

        managedUser.setWrongTime(4);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 60 * 1000L));

        managedUser.setWrongTime(5);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 5 * 60 * 1000L));

        managedUser.setWrongTime(6);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 10 * 60 * 1000L));

        managedUser.setWrongTime(7);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 30 * 60 * 1000L));

        managedUser.setWrongTime(8);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 24 * 3600 * 1000L));

        managedUser.setWrongTime(9);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 72 * 3600 * 1000L));

        managedUser.setWrongTime(10);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 72 * 3600 * 1000L));
    }

    @Test
    public void testGetLockLeftSeconds() {

        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");

        managedUser.setWrongTime(3);
        Assert.assertEquals(10, UserLockRuleUtil.getLockLeftSeconds(managedUser, 20 * 1000L));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 30 * 1000L - 2));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 30 * 1000L));

        managedUser.setWrongTime(9);
        Assert.assertEquals(72 * 3600 - 20, UserLockRuleUtil.getLockLeftSeconds(managedUser, 20 * 1000L));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 72 * 3600 * 1000L - 2));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 72 * 3600 * 1000L));
    }
}
