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

package io.kyligence.kap.rest.config;

import javax.servlet.Filter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Order(200)
@Configuration
@EnableWebSecurity
@Profile({ "testing", "ldap", "custom" })
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    @Autowired
    Filter fillEmptyAuthorizationFilter;

    @Autowired
    LogoutSuccessHandler logoutSuccessHandler;

    @Autowired
    AuthenticationEntryPoint nUnauthorisedEntryPoint;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        // @formatter:off
        // https://docs.spring.io/spring-security/site/docs/3.2.x/reference/htmlsingle/html5/#nsa-http-attributes
        http.formLogin()
                .and()
                .httpBasic()
                .authenticationEntryPoint(nUnauthorisedEntryPoint)
                .and()
                .logout();

        // https://spring.io/blog/2013/07/11/spring-security-java-config-preview-readability/
        // use-expressions="true"

        http.headers()
                .frameOptions(HeadersConfigurer.FrameOptionsConfig::sameOrigin)
                .and()
                .csrf(AbstractHttpConfigurer::disable)
                .addFilterBefore(fillEmptyAuthorizationFilter, BasicAuthenticationFilter.class);

        http.authorizeRequests()
                .antMatchers("/api/streaming_jobs/spark", "/api/streaming_jobs/stats",
                "/api/streaming_jobs/dataflow/**", "/api/epoch/maintenance_mode", "/api/health", "/api/health/**",
                "/api/prometheus", "/api/monitor/spark/prometheus", "/api/user/update_user", "/api/metastore/cleanup",
                "/api/metastore/cleanup_storage", "/api/epoch", "/api/broadcast/**", "/api/config/is_cloud",
                "/api/system/license/file", "/api/system/license/content", "/api/system/license/trial",
                "/api/system/license", "/api/system/diag/progress", "/api/system/roll_event_log",
                "/api/user/authentication*/**", "/api/query/history_queries/table_names", "/api/models/model_info",
                "/api/**/metrics", "/api/system/backup", "/api/jobs/spark", "/api/jobs/stage/status", "/api/jobs/error",
                "/api/jobs/wait_and_run_time", "/api/cache*/**", "/api/admin/public_config",
                "/api/admin/instance_info", "/api/projects", "/api/system/license/info")
                .permitAll()
                .and()
                .authorizeRequests()
                .antMatchers("/api/monitor/alert", "/api/admin*/**")
                .access("hasRole('ROLE_ADMIN')")
                .and()
                .authorizeRequests()
                .antMatchers("/api/cubes/src/tables")
                .access("hasRole('ROLE_ANALYST')")
                .and()
                .authorizeRequests()
                .antMatchers("/api/query*/**", "/api/metadata*/**", "/api/cubes*/**", "/api/models*/**", "/api/job*/**",
                        "/api/**")
                .authenticated();

        http.logout()
                .invalidateHttpSession(true)
                .deleteCookies("JSESSIONID")
                .logoutUrl("/api/j_spring_security_logout")
                .logoutSuccessHandler(logoutSuccessHandler);

        http.sessionManagement(configurer -> configurer.sessionFixation().newSession());
        // @formatter:on
    }
}
