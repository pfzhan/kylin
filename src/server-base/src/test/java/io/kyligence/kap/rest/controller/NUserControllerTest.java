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



package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.rest.service.LicenseInfoService.getDefaultCommitFile;
import static org.apache.kylin.rest.service.LicenseInfoService.getDefaultLicenseFile;
import static org.apache.kylin.rest.service.LicenseInfoService.getDefaultVersionFile;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.constant.Constants;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import io.kyligence.kap.rest.rules.ClearKEPropertiesRule;
import io.kyligence.kap.rest.service.AclTCRService;
import lombok.val;

public class NUserControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    private static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    @Rule
    public ClearKEPropertiesRule clearKEProperties = new ClearKEPropertiesRule();

    @InjectMocks
    private NUserController nUserController;

    @Mock
    private UserService userService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private IUserGroupService userGroupService;

    @Mock
    Environment env;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AccessService accessService;

    private LicenseInfoService licenseInfoService = new LicenseInfoService();

    @Before
    public void setupResource() {
        super.createTestMetadata();
        getTestConfig().setProperty("kylin.env", "UT");
        nUserController = Mockito.spy(new NUserController());
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(true).when(env).acceptsProfiles("testing", "custom");
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserController)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "ADMIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(nUserController, "licenseInfoService", licenseInfoService);
        ReflectionTestUtils.setField(nUserController, "userGroupService", userGroupService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        EnvelopeResponse<UserDetails> userDetailsEnvelopeResponse = nUserController.authenticatedUser();
        Assert.assertNotNull(userDetailsEnvelopeResponse);
        Assert.assertEquals(userDetailsEnvelopeResponse.getCode(), KylinException.CODE_SUCCESS);
    }

    @Test
    @Ignore("verify license code is in a individual patch")
    public void testAuthenticated_WithOutOfDateLicense() {
        getTestConfig().setProperty("kylin.env", "PROD");
        licenseInfoService.gatherLicenseInfo(getDefaultLicenseFile(), getDefaultCommitFile(), getDefaultVersionFile(),
                null);
        overwriteSystemProp(Constants.KE_DATES, "2018-12-17,2019-01-17");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getLICENSE_OVERDUE_TRIAL(), "2018-12-17",
                "2019-01-17"));
        nUserController.authenticate();
    }

    @Test
    @Ignore("verify license code is in a individual patch")
    public void testAuthenticated_WithoutLicense() {
        getTestConfig().setProperty("kylin.env", "PROD");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(Message.getInstance().getLICENSE_INVALID_LICENSE());
        nUserController.authenticate();
    }

    @Test
    public void testCreateUser() throws Exception {
        val user = new ManagedUser();
        user.setUsername("azAZ_#");
        user.setPassword("p14532522?");
        userGroupService.addGroup(Constant.GROUP_ALL_USERS);
        List<SimpleGrantedAuthority> groups = Lists.newArrayList(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS));
        user.setGrantedAuthorities(groups);
        Mockito.doReturn(true).when(userGroupService).exists(Constant.GROUP_ALL_USERS);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).createUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testCreateUserWithEmptyUsername() throws IOException {
        val user = new ManagedUser();
        user.setUsername("");
        user.setPassword("p1234sgw$");

        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "Username should not be empty.");

        user.setUsername(".abc");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "The user / group names cannot start with a period");

        user.setUsername(" abc ");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "User / group names cannot start or end with a space");

        user.setUsername(" abc ");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "User / group names cannot start or end with a space");

        user.setUsername("useraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, Message.getInstance().getINVALID_NAME_LEGTHN());

        user.setUsername("<1qaz>");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "The user / group names cannot contain the following symbols:");
    }

    @Test
    public void testCreateUserWithEmptyGroup() throws IOException {
        val user = new ManagedUser();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        thrown.expect(KylinException.class);
        thrown.expectMessage("User group name should not be empty.");
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithNotExistGroup() throws IOException {
        val user = new ManagedUser();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        userGroupService.addGroup(Constant.GROUP_ALL_USERS);
        List<SimpleGrantedAuthority> groups = Lists.newArrayList(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS));
        user.setGrantedAuthorities(groups);
        Mockito.doReturn(false).when(userGroupService).exists(Constant.GROUP_ALL_USERS);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Operation failed, group:[ALL_USERS] not exists, please add it first");
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithDuplicatedGroup() throws IOException {
        val user = new ManagedUser();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        userGroupService.addGroup(Constant.GROUP_ALL_USERS);
        List<SimpleGrantedAuthority> groups = Lists.newArrayList(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS),
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS));
        Mockito.doReturn(true).when(userGroupService).exists(Constant.GROUP_ALL_USERS);
        user.setGrantedAuthorities(groups);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Values in authorities can't be duplicated.");
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithNotEnglishUsername() throws IOException {
        val user = new ManagedUser();
        user.setUsername("中文");
        user.setPassword("p1234sgw$");
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_NAME_CONTAINS_OTHER_CHARACTER());
        nUserController.createUser(user);
    }

    @Test(expected = KylinException.class)
    public void testCreateUserWithIlegalCharacter() throws IOException {
        val user = new ManagedUser();
        user.setUsername("gggg?k");
        user.setPassword("p1234sgw$");
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUser_PasswordLength_Exception() throws Exception {
        val user = new ManagedUser();
        user.setUsername("u1");
        user.setPassword("p1");
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("The password should contain more than 8 characters!")));

        Mockito.verify(nUserController).createUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testCreateUser_InvalidPasswordPattern() throws Exception {
        val user = new ManagedUser();
        user.setUsername("u1");
        user.setPassword("kylin123456");
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(
                        "The password should contain at least one number, letter and special character (~!@#$%^&*(){}|:\\\"<>?[];\\\\'\\\\,./`).")));

        Mockito.verify(nUserController).createUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testDelUserByUUID() throws Exception {
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername("username1");

        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user/{uuid:.+}", "u1")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).deleteByUUID("u1");
    }

    @Test
    public void testDelUserByUsername() throws Exception {
        String username = "username1";
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername(username);

        Mockito.doReturn(user).when(userService).loadUserByUsername(username);
        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());

        nUserController.delete(username);
    }

    @Test
    public void testDelUserByUsernameException() throws Exception {
        String username = "username1";
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername(username);

        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, "User '%s' not found.", username));
        nUserController.delete(username);
    }

    @Test
    public void testUpdatePassword_UserNotFound() throws Exception {
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString("User 'ADMIN' not found.")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_Success() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_OldSameAsNew() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN1234@"), false);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN1234@");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("New password should not be same as old one!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_InvalidPasswordPattern() throws Exception {
        val user = new ManagedUser();
        val request = new PasswordChangeRequest();

        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("kylin123456");
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(
                        "The password should contain at least one number, letter and special character")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_InvalidPasswordLength() throws Exception {
        val user = new ManagedUser();
        val request = new PasswordChangeRequest();

        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("123456");
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("The password should contain more than 8 characters!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_Success() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "MODELER", Constant.ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_WrongPassword() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "ANALYST", Constant.ROLE_ANALYST);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN1");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString("Old password is not correct!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testListAll() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user").contentType(MediaType.APPLICATION_JSON)
                .param("name", "KYLIN").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).listAllUsers(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
                Mockito.anyInt());
    }

    @Test
    public void testUpdateUser() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);

        Mockito.doReturn(true).when(userGroupService).exists("ALL_USERS");
        Mockito.doReturn(new HashSet<String>(){{add(Constant.GROUP_ALL_USERS);}}).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testUpdateUserWithDuplicatedGroup() throws Exception {
        val user = new ManagedUser();
        user.setUsername("ADMIN");
        user.setPassword("KYLIN1234@");
        userGroupService.addGroup(Constant.GROUP_ALL_USERS);
        List<SimpleGrantedAuthority> groups = Lists.newArrayList(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS),
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS));
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        Mockito.doReturn(new HashSet<String>(){{add(Constant.GROUP_ALL_USERS);}}).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(true).when(userGroupService).exists(Constant.GROUP_ALL_USERS);
        user.setGrantedAuthorities(groups);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Values in authorities can't be duplicated.");
        nUserController.updateUser(user);
    }
    @Test
    public void testUpdateOwnUserType() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        HashSet<String> groups = new HashSet<String>() {{
            add(Constant.GROUP_ALL_USERS);
            add(Constant.ROLE_ADMIN);
        }};
        Mockito.doReturn(groups).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        Mockito.doReturn(true).when(userGroupService).exists(Constant.GROUP_ALL_USERS);
        Mockito.doReturn(true).when(userGroupService).exists(Constant.ROLE_ADMIN);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nUserController).updateUser(Mockito.any(ManagedUser.class));
    }
}
