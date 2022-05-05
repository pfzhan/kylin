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

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.metadata.PersistException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;

public class AdminUserInitCLI {
    protected static final Logger logger = LoggerFactory.getLogger(AdminUserInitCLI.class);

    public static final String ADMIN_USER_NAME = "ADMIN";
    public static final String ADMIN_USER_RES_PATH = "/_global/user/ADMIN";

    public static final Pattern PASSWORD_PATTERN = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");

    public static final String PASSWORD_VALID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
            + "~!@#$%^&*(){}|:\"<>?[];',./`";

    public static final int DEFAULT_PASSWORD_LENGTH = 8;

    public static void main(String[] args) {
        try {
            boolean randomPasswordEnabled = KylinConfig.getInstanceFromEnv().getRandomAdminPasswordEnabled();
            initAdminUser(randomPasswordEnabled);
        } catch (Exception e) {
            logger.error("Create Admin user failed.", e);
            Unsafe.systemExit(1);
        }
        Unsafe.systemExit(0);
    }

    public static void initAdminUser(boolean randomPasswordEnabled) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();

        if ("ldap".equalsIgnoreCase(config.getSecurityProfile()) && !config.isRemoveLdapCustomSecurityLimitEnabled()) {
            return;
        }

        NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        if (!randomPasswordEnabled) {
            return;
        }

        if (CollectionUtils.isNotEmpty(userManager.list())) {
            logger.info("The user has been initialized and does not need to be initialized again");
            return;
        }

        String password = generateRandomPassword();

        ManagedUser managedUser = new ManagedUser(ADMIN_USER_NAME,
                PasswordEncodeFactory.newUserPasswordEncoder().encode(password), true, ROLE_ADMIN,
                Constant.GROUP_ALL_USERS);
        managedUser.setUuid(RandomUtil.randomUUIDStr());

        val metaStore = ResourceStore.getKylinMetaStore(config).getMetadataStore();
        try {
            logger.info("Start init default user.");
            RawResource rawResource = new RawResource(ADMIN_USER_RES_PATH,
                    ByteSource.wrap(JsonUtil.writeValueAsBytes(managedUser)), System.currentTimeMillis(), 0L);
            metaStore.putResource(rawResource, null, UnitOfWork.DEFAULT_EPOCH_ID);

            String blackColorUsernameForPrint = StorageCleaner.ANSI_RESET + ADMIN_USER_NAME + StorageCleaner.ANSI_RED;
            String blackColorPasswordForPrint = StorageCleaner.ANSI_RESET + password + StorageCleaner.ANSI_RED;
            String info = String.format(Locale.ROOT,
                    "Create default user finished. The username of initialized user is [%s], which password is [%s].\n"
                            + "Please keep the password properly. And if you forget the password, you can reset it according to user manual.",
                    blackColorUsernameForPrint, blackColorPasswordForPrint);
            System.out.println(StorageCleaner.ANSI_RED + info + StorageCleaner.ANSI_RESET);
        } catch (PersistException e) {
            logger.warn("{} user has been created on another node.", ADMIN_USER_NAME);
        }
    }

    public static String generateRandomPassword() {
        String password;
        do {
            password = RandomStringUtils.random(DEFAULT_PASSWORD_LENGTH, PASSWORD_VALID_CHARS.toCharArray());
        } while (!PASSWORD_PATTERN.matcher(password).matches());
        return password;
    }
}
