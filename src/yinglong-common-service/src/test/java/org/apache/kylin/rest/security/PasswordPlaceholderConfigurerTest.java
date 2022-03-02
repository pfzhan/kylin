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

import java.io.IOException;
import java.util.Properties;

import org.apache.kylin.rest.exception.PasswordDecryptionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kyligence.kap.common.util.EncryptUtil;
import io.kyligence.kap.junit.annotation.MetadataInfo;

@MetadataInfo(onlyProps = true)
public class PasswordPlaceholderConfigurerTest {

    @Test
    public void getAllKylinProperties() throws IOException {
        PasswordPlaceholderConfigurer passwordPlaceholderConfigurer = new PasswordPlaceholderConfigurer();
        passwordPlaceholderConfigurer.getAllKylinProperties();
    }

    @Test
    public void resolvePlaceholder() throws IOException {
        String password = "123456";
        String encryptedPassword = EncryptUtil.encrypt(password);
        Properties properties = new Properties();
        properties.setProperty("kylin.security.ldap.connection-password", encryptedPassword);
        properties.setProperty("kylin.security.user-password-encoder", password);
        PasswordPlaceholderConfigurer passwordPlaceholderConfigurer = new PasswordPlaceholderConfigurer();
        Assertions.assertEquals(password,
                passwordPlaceholderConfigurer.resolvePlaceholder("kylin.security.user-password-encoder", properties));
        Assertions.assertEquals(password, passwordPlaceholderConfigurer
                .resolvePlaceholder("kylin.security.ldap.connection-password", properties));

        properties.setProperty("kylin.security.ldap.connection-password", password);
        Assertions.assertThrows(PasswordDecryptionException.class, () -> passwordPlaceholderConfigurer
                .resolvePlaceholder("kylin.security.ldap.connection-password", properties));
    }

    @Test
    public void main() {
        String[] args = new String[] { "AES", "PASSWORD" };
        PasswordPlaceholderConfigurer.main(args);
        args = new String[] { "BCrypt", "PASSWORD" };
        PasswordPlaceholderConfigurer.main(args);
    }
}