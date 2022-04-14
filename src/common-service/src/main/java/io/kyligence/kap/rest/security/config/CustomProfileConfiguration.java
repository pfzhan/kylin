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

package io.kyligence.kap.rest.security.config;

import java.lang.reflect.InvocationTargetException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationProvider;

import io.kyligence.kap.rest.service.OpenUserGroupService;
import io.kyligence.kap.rest.service.OpenUserService;

@Profile("custom")
@Configuration
public class CustomProfileConfiguration {

    @Value("${kylin.security.custom.user-service-class-name}")
    private String userClassName;

    @Value("${kylin.security.custom.user-group-service-class-name}")
    private String userGroupClassName;

    @Value("${kylin.security.custom.authentication-provider-class-name}")
    private String authenticationProviderClassName;

    @Bean("userService")
    public OpenUserService userService() throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Class<?> serviceClass = Class.forName(userClassName);
        return (OpenUserService) serviceClass.getDeclaredConstructor().newInstance();
    }

    @Bean("userGroupService")
    public OpenUserGroupService userGroupService() throws ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, NoSuchMethodException {
        Class<?> serviceClass = Class.forName(userGroupClassName);
        return (OpenUserGroupService) serviceClass.getDeclaredConstructor().newInstance();
    }

    @Bean("customAuthProvider")
    public AuthenticationProvider customAuthProvider() throws ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, NoSuchMethodException {
        Class<?> serviceClass = Class.forName(authenticationProviderClassName);
        return (AuthenticationProvider) serviceClass.getDeclaredConstructor().newInstance();
    }
}
