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

import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.springframework.ldap.core.ContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;

import com.google.common.collect.Sets;

import javax.naming.directory.SearchControls;

public class LdapAuthoritiesPopulator extends DefaultLdapAuthoritiesPopulator {

    private SimpleGrantedAuthority adminRoleAsAuthority;

    public LdapAuthoritiesPopulator(ContextSource contextSource, String groupSearchBase, String adminRole,
                                    SearchControls searchControls) {
        super(contextSource, groupSearchBase);
        setConvertToUpperCase(false);
        setRolePrefix("");
        this.adminRoleAsAuthority = new SimpleGrantedAuthority(adminRole);
        this.getLdapTemplate().setSearchControls(searchControls);
    }

    @Override
    public Set<GrantedAuthority> getGroupMembershipRoles(String userDn, String username) {
        Set<GrantedAuthority> authorities = super.getGroupMembershipRoles(userDn, username);
        Set<GrantedAuthority> userAuthorities = Sets.newHashSet(authorities);
        if (authorities.contains(adminRoleAsAuthority)) {
            userAuthorities.add(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
        }
        return userAuthorities;
    }
}
