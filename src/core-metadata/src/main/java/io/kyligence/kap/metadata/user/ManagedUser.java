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



package io.kyligence.kap.metadata.user;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import io.kyligence.kap.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.insensitive.UserInsensitiveRequest;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Getter
@Setter
@JsonFilter("passwordFilter")
@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = {"AuthenticationClient", "StaticUserGroupService", "StaticUserService"})
})
public class ManagedUser extends RootPersistentEntity implements UserDetails, UserInsensitiveRequest {

    // for spring session save serializable object(ManagerUser), do not modify
    private static final long serialVersionUID = 1L;

    @JsonProperty
    private String username;
    @JsonProperty
    private String password;
    @JsonProperty
    @JsonSerialize(using = SimpleGrantedAuthoritySerializer.class)
    @JsonDeserialize(using = SimpleGrantedAuthorityDeserializer.class)
    private List<SimpleGrantedAuthority> authorities = Lists.newArrayList();
    @JsonProperty
    private boolean disabled = false;
    @JsonProperty
    private boolean defaultPassword = false;
    @JsonProperty
    private boolean locked = false;
    @JsonProperty("locked_time")
    private long lockedTime = 0L;
    @JsonProperty("wrong_time")
    private int wrongTime = 0;
    @JsonProperty("first_login_failed_time")
    private long firstLoginFailedTime = 0L;

    //DISABLED_ROLE is a ancient way to represent disabled user
    //now we no longer support such way, however legacy metadata may still contain it
    private static final String DISABLED_ROLE = "--disabled--";
    private static final SimpleGrantedAuthority DEFAULT_GROUP = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    public ManagedUser() {
    }

    public ManagedUser(@JsonProperty String username, @JsonProperty String password,
            @JsonProperty List<SimpleGrantedAuthority> authorities, @JsonProperty boolean disabled,
            @JsonProperty boolean defaultPassword, @JsonProperty boolean locked, @JsonProperty long lockedTime,
            @JsonProperty int wrongTime) {
        this.username = username;
        this.password = password;
        this.authorities = authorities;
        this.disabled = disabled;
        this.defaultPassword = defaultPassword;
        this.locked = locked;
        this.lockedTime = lockedTime;
        this.wrongTime = wrongTime;

        caterLegacy();

        this.setUuid(UUID.randomUUID().toString());
    }

    public ManagedUser(String username, String password, Boolean defaultPassword, String... authoritiesStr) {
        this.username = username;
        this.password = password;
        this.setDefaultPassword(defaultPassword);

        this.authorities = Lists.newArrayList();
        for (String a : authoritiesStr) {
            authorities.add(new SimpleGrantedAuthority(a));
        }
        // user must belong to all users.
        if (!authorities.contains(DEFAULT_GROUP)) {
            authorities.add(DEFAULT_GROUP);
        }
        caterLegacy();

        this.setUuid(UUID.randomUUID().toString());
    }

    public ManagedUser(String username, String password, Boolean defaultPassword,
            Collection<? extends GrantedAuthority> grantedAuthorities) {
        this.username = username;
        this.password = password;
        this.setDefaultPassword(defaultPassword);
        this.setGrantedAuthorities(grantedAuthorities);
        // user must belong to all users.
        if (!authorities.contains(DEFAULT_GROUP)) {
            authorities.add(DEFAULT_GROUP);
        }
        caterLegacy();

        this.setUuid(UUID.randomUUID().toString());
    }

    @Override
    public String resourceName() {
        return username;
    }

    private void caterLegacy() {
        Iterator<SimpleGrantedAuthority> iterator = authorities.iterator();
        while (iterator.hasNext()) {
            if (DISABLED_ROLE.equals(iterator.next().getAuthority())) {
                iterator.remove();
                this.disabled = true;
            }
        }
    }

    public List<SimpleGrantedAuthority> getAuthorities() {
        return this.authorities;
    }

    public void setGrantedAuthorities(Collection<? extends GrantedAuthority> grantedAuthorities) {
        this.authorities = Lists.newArrayList();
        for (GrantedAuthority grantedAuthority : grantedAuthorities) {
            this.authorities.add(new SimpleGrantedAuthority(grantedAuthority.getAuthority()));
        }
    }

    public void addAuthorities(String auth) {
        if (this.authorities == null) {
            this.authorities = Lists.newArrayList();
        }
        authorities.add(new SimpleGrantedAuthority(auth));
    }

    public void removeAuthorities(String auth) {
        Preconditions.checkNotNull(this.authorities == null);
        authorities.remove(new SimpleGrantedAuthority(auth));
    }

    public void increaseWrongTime() {
        int wrongTime = this.getWrongTime();
        if (wrongTime >= 2) {
            this.setLocked(true);
            this.lockedTime = System.currentTimeMillis();
        }
        this.wrongTime = wrongTime + 1;
    }

    public void authenticateFail() {
        long currentTime = System.currentTimeMillis();

        if (this.firstLoginFailedTime == 0) {
            this.firstLoginFailedTime = currentTime;
        }
        increaseWrongTime();
    }

    public void clearAuthenticateFailedRecord() {
        this.firstLoginFailedTime = 0;
        this.wrongTime = 0;
        this.locked = false;
        this.lockedTime = 0;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return !locked;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return !disabled;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ManagedUser other = (ManagedUser) obj;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ManagedUser [username=" + username + ", authorities=" + authorities + "]";
    }

    public static class SimpleGrantedAuthoritySerializer extends JsonSerializer<List<SimpleGrantedAuthority>> {

        @Override
        public void serialize(List<SimpleGrantedAuthority> value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            List<UserGrantedAuthority> ugaList = Lists.newArrayList();
            for (SimpleGrantedAuthority sga : value) {
                ugaList.add(new UserGrantedAuthority(sga.getAuthority()));
            }

            gen.writeObject(ugaList);
        }
    }

    public static class SimpleGrantedAuthorityDeserializer extends JsonDeserializer<List<SimpleGrantedAuthority>> {

        @Override
        public List<SimpleGrantedAuthority> deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            UserGrantedAuthority[] ugaArray = p.readValueAs(UserGrantedAuthority[].class);
            List<SimpleGrantedAuthority> sgaList = Lists.newArrayList();
            for (UserGrantedAuthority uga : ugaArray) {
                sgaList.add(new SimpleGrantedAuthority(uga.getAuthority()));
            }

            return sgaList;
        }
    }
}
