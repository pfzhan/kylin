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

package org.apache.kylin.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.kyligence.kap.metadata.user.ManagedUser;
import lombok.Getter;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;

@Getter
public class UserInfoResponse {
    @JsonProperty("username")
    private String username;
    @JsonProperty("authorities")
    @JsonSerialize(using = ManagedUser.SimpleGrantedAuthoritySerializer.class)
    @JsonDeserialize(using = ManagedUser.SimpleGrantedAuthorityDeserializer.class)
    private List<SimpleGrantedAuthority> authorities;
    @JsonProperty("disabled")
    private boolean disabled;
    @JsonProperty("default_password")
    private boolean defaultPassword;
    @JsonProperty("locked")
    private boolean locked;
    @JsonProperty("uuid")
    protected String uuid;
    @JsonProperty("last_modified")
    protected long lastModified;
    @JsonProperty("create_time")
    protected long createTime;
    @JsonProperty("locked_time")
    private long lockedTime;
    @JsonProperty("wrong_time")
    private int wrongTime;
    @JsonProperty("first_login_failed_time")
    private long firstLoginFailedTime;


    public UserInfoResponse(ManagedUser managedUser) {
        this.username = managedUser.getUsername();
        this.authorities = managedUser.getAuthorities();
        this.disabled = managedUser.isDisabled();
        this.defaultPassword = managedUser.isDefaultPassword();
        this.locked = managedUser.isLocked();
        this.uuid = managedUser.getUuid();
        this.lastModified = managedUser.getLastModified();
        this.createTime = managedUser.getCreateTime();
        this.lockedTime = managedUser.getLockedTime();
        this.wrongTime = managedUser.getWrongTime();
        this.firstLoginFailedTime = managedUser.getFirstLoginFailedTime();
    }
}
