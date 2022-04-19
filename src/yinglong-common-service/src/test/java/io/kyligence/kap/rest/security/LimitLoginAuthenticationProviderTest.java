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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;

public class LimitLoginAuthenticationProviderTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private LimitLoginAuthenticationProvider limitLoginAuthenticationProvider;

    @Mock
    private ServletRequestAttributes attrs;

    private UserService userService = new KylinUserService();

    @InjectMocks
    private KylinUserService kylinUserService = Mockito.spy(new KylinUserService());

    private ManagedUser userAdmin = new ManagedUser("ADMIN", "KYLIN", false, Constant.ROLE_ADMIN);

    private ManagedUser userModeler = new ManagedUser("MODELER", "MODELER", false, Constant.ROLE_MODELER);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        RequestContextHolder.setRequestAttributes(attrs);
        limitLoginAuthenticationProvider = Mockito.spy(new LimitLoginAuthenticationProvider());
        // spring security 5 has removed PlainTextPasswordEncoder
        // https://github.com/spring-projects/spring-security/blob/4.2.x/core/src/main/java/
        // org/springframework/security/authentication/encoding/PlaintextPasswordEncoder.java
        limitLoginAuthenticationProvider.setPasswordEncoder(NoOpPasswordEncoder.getInstance());
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userService", userService);
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userDetailsService", userService);
        kylinUserService.updateUser(userAdmin);
        kylinUserService.updateUser(userModeler);
    }

    @Test
    public void testAuthenticate_UserNotFound_EmptyUserName() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("", userAdmin.getPassword(),
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_UserNotFound_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("lalala",
                userAdmin.getPassword(), userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_InSensitiveCase() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("admin", "KYLIN",
                userAdmin.getAuthorities());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_EmptyPassword() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_WrongPWD_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "fff",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_Locked_Exception() {
        userAdmin.setLocked(true);
        userAdmin.setLockedTime(System.currentTimeMillis());
        userAdmin.setWrongTime(3);
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        try {
            limitLoginAuthenticationProvider.authenticate(token);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LockedException);
            String msg = e.getMessage();
            Assert.assertTrue(msg
                    .matches("For security concern, account ADMIN has been locked. Please try again in \\d+ seconds. "
                            + "Login failure again will be locked for 1 minutes.."));
        }
    }

    @Test
    public void testAuthenticate_Disabled_Exception() {
        userAdmin.setDisabled(true);
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        thrown.expect(DisabledException.class);
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testPbkdf2PasswordEncoder() {
        limitLoginAuthenticationProvider.setPasswordEncoder(new Pbkdf2PasswordEncoder());
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        limitLoginAuthenticationProvider.authenticate(token);
        limitLoginAuthenticationProvider.setPasswordEncoder(new BCryptPasswordEncoder());
    }

}
