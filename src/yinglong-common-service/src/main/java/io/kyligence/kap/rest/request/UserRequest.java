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


package io.kyligence.kap.rest.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.insensitive.UserInsensitiveRequest;
import io.kyligence.kap.metadata.user.ManagedUser;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;
import java.util.stream.Collectors;

import static io.kyligence.kap.metadata.user.ManagedUser.DEFAULT_GROUP;
import static io.kyligence.kap.metadata.user.ManagedUser.DISABLED_ROLE;

@SuppressWarnings("serial")
@Data
public class UserRequest implements UserInsensitiveRequest {

    @JsonProperty
    private String username;
    @JsonProperty
    private String password;
    @JsonProperty
    private List<String> authorities = Lists.newArrayList();
    @JsonProperty
    private Boolean disabled;
    @JsonProperty
    private Boolean defaultPassword;

    public UserRequest() {
    }

    public ManagedUser updateManager(ManagedUser managedUser) {
        if (disabled != null) {
            managedUser.setDisabled(disabled);
        }
        if (defaultPassword != null) {
            managedUser.setDefaultPassword(defaultPassword);
        }
        if (!StringUtils.isEmpty(password))
            managedUser.setPassword(password);
        if (authorities != null && !authorities.isEmpty()) {
            if (authorities.stream().anyMatch(authority -> DISABLED_ROLE.equals(authority))) {
                managedUser.setDisabled(true);
                authorities.remove(DISABLED_ROLE);
            }
            List<SimpleGrantedAuthority> authorities = this.authorities.stream().map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toList());
            if (!authorities.contains(DEFAULT_GROUP)) {
                authorities.add(DEFAULT_GROUP);
            }
            managedUser.setAuthorities(authorities);
        }

        return managedUser;
    }

}