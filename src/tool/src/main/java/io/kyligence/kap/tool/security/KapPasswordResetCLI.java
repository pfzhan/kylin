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

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.tool.MetadataTool;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.tool.CuratorOperator;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;

public class KapPasswordResetCLI {
    protected static final Logger logger = LoggerFactory.getLogger(KapPasswordResetCLI.class);

    public static void main(String[] args) {
        try (val curatorOperator = new CuratorOperator()) {
            if (!curatorOperator.isJobNodeExist()) {
                reset();
                System.exit(0);
            } else {
                logger.warn("Fail to reset admin password, please stop all job nodes first");
            }
        } catch (Exception e) {
            logger.warn("Fail to reset admin password.", e);
        }
        System.exit(1);
    }

    public static void reset() throws Exception {
        BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();
        String id = "/_global/user/ADMIN";
        val config = KylinConfig.getInstanceFromEnv();

        ResourceStore aclStore = ResourceStore.getKylinMetaStore(config);
        val metaStore = aclStore.getMetadataStore();
        NKylinUserManager userManager = NKylinUserManager.getInstance(config);

        val user = userManager.get("ADMIN");
        if (user == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            System.exit(1);
        }
        boolean randomPasswordEnabled = KylinConfig.getInstanceFromEnv().getRandomAdminPasswordEnabled();
        String password = randomPasswordEnabled ? AdminUserInitCLI.generateRandomPassword() : "KYLIN";
        user.setPassword(pwdEncoder.encode(password));
        user.setDefaultPassword(true);

        val res = aclStore.getResource(id);

        if (res == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            System.exit(1);
        }

        metaStore.putResource(new RawResource(id, ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(user)),
                aclStore.getResource(id).getTimestamp(), aclStore.getResource(id).getMvcc() + 1), null, 0L, UnitOfWork.DEFAULT_EPOCH_ID);

        logger.trace("update user : {}", user.getUsername());
        logger.info("User {}'s password is set to default password.", user.getUsername());

        MetadataTool.backup(config);

        if (randomPasswordEnabled) {
            String blackColorUsernameForPrint = StorageCleaner.ANSI_RESET + AdminUserInitCLI.ADMIN_USER_NAME + StorageCleaner.ANSI_RED;
            String blackColorPasswordForPrint = StorageCleaner.ANSI_RESET + password + StorageCleaner.ANSI_RED;
            String info = String.format("Reset password of [%s] succeed. The password is [%s].\n" +
                    "Please keep the password properly.", blackColorUsernameForPrint, blackColorPasswordForPrint);
            System.out.println(StorageCleaner.ANSI_RED + info + StorageCleaner.ANSI_RESET);
        } else {
            System.out.println(StorageCleaner.ANSI_YELLOW + "Reset the ADMIN password successfully." + StorageCleaner.ANSI_RESET);
        }
    }
}
