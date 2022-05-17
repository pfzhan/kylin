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

import java.util.List;
import java.util.Locale;

import io.kyligence.kap.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.service.UserService;
import org.springframework.security.core.userdetails.UserDetails;

import io.kyligence.kap.metadata.user.ManagedUser;

@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = { "StaticUserService"})
})
public abstract class OpenUserService implements UserService {

    @Override
    public abstract List<ManagedUser> listUsers();

    @Override
    public abstract List<String> listAdminUsers();

    @Override
    public void createUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "createUser"));
    }

    @Override
    public void updateUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "updateUser"));
    }

    @Override
    public void deleteUser(String s) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "deleteUser"));
    }

    @Override
    public void changePassword(String s, String s1) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "changePassword"));
    }

    @Override
    public abstract UserDetails loadUserByUsername(String s);
}
