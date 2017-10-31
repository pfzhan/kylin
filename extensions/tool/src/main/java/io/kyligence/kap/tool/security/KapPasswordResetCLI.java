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

package io.kyligence.kap.tool.security;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import io.kyligence.kap.tool.storage.KapTestJdbcCLI;

public class KapPasswordResetCLI {
    protected static final Logger logger = LoggerFactory.getLogger(KapTestJdbcCLI.class);


    public static void main(String[] args) throws Exception {
        BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();
        String DIR_PREFIX = "/user/";
        ResourceStore aclStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

        ManagedUser user = new ManagedUser("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.ROLE_ANALYST,
                Constant.ROLE_MODELER);
        user.setPassword(pwdEncoder.encode(user.getPassword()));
        ManagedUser managedUser = (ManagedUser) user;

        try {
            String id = DIR_PREFIX + user.getUsername();
            aclStore.putResourceWithoutCheck(id, managedUser, System.currentTimeMillis(), SERIALIZER);
            logger.trace("update user : {}", user.getUsername());
            System.out.println("User " + user.getUsername() + "'s password is set to default password.");
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }
}
