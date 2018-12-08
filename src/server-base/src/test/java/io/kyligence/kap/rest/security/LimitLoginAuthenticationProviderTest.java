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

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
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

import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public class LimitLoginAuthenticationProviderTest extends ServiceTestBase {

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
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        RequestContextHolder.setRequestAttributes(attrs);
        limitLoginAuthenticationProvider = Mockito.spy(new LimitLoginAuthenticationProvider());
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userService", userService);
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userDetailsService", userService);
        kylinUserService.updateUser(userAdmin);
        kylinUserService.updateUser(userModeler);
    }

    @Test
    public void testAuthenticate_UserNotFound_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("lalala",
                userAdmin.getPassword(), userAdmin.getAuthorities());
        thrown.expect(UsernameNotFoundException.class);
        thrown.expectMessage("User 'lalala' not found.");
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_WrongPWD_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "fff",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage("Invalid username or password.");
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_Locked_Exception() {
        userAdmin.setLocked(true);
        userAdmin.setLockedTime(System.currentTimeMillis());
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        thrown.expect(LockedException.class);
        thrown.expectMessage("User ADMIN is locked, please try again after 30 seconds.");
        limitLoginAuthenticationProvider.authenticate(token);
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

}
