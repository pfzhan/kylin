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

import com.google.common.collect.Lists;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KapAuthenticationManager {

    private static final Logger logger = LoggerFactory.getLogger(KapAuthenticationManager.class);


    private Map<String, UserObj> userObjMap;

    private KapAuthenticationManager() {
        userObjMap = new HashMap<String, UserObj>();
    }

    private static KapAuthenticationManager manager = null;

    public static KapAuthenticationManager getManager() {
        if (manager == null) {
            manager = new KapAuthenticationManager();
        }
        return manager;
    }

    public void addUser(List<UserObj> users) {
        for (UserObj u : users) {
            userObjMap.put(u.getUsername(), u);
            logger.info(u.toString());
        }
    }

    public void addUser(UserObj user) {
        userObjMap.put(user.getUsername(), user);
    }

    public void removeUser(String userName) {
        userObjMap.remove(userName);
    }

    public boolean isUserLocked(String userName) {
        boolean locked = false;
        if (userObjMap.get(userName) != null)
            locked = userObjMap.get(userName).isLocked();
        return locked;
    }

    public void lockUser(String userName) {
        UserObj user = userObjMap.get(userName);
        if (user != null)
            user.setLocked(true);
    }

    public void unlockUser(String userName) {
        UserObj user = userObjMap.get(userName);
        if (user != null)
            user.setLocked(false);
    }

    public long getLockedTime(String userName) {
        long lockedTime = 0L;
        if (userObjMap.get(userName) != null)
            lockedTime = userObjMap.get(userName).getLockedTime();
        return lockedTime;
    }

    public void setLockedTime(String userName) {
        UserObj user = userObjMap.get(userName);
        if (user != null)
            user.setLockedTime(System.currentTimeMillis());
    }

    public int getWrongTime(String userName) {
        int wrongTime = 0;
        if (userObjMap.get(userName) != null)
            wrongTime = userObjMap.get(userName).getWrongTime();
        return wrongTime;
    }

    public void increaseWrongTime(String userName) {
        UserObj user = userObjMap.get(userName);
        if (user != null) {
            int wrongTime = user.getWrongTime();
            if (wrongTime == 2) {
                lockUser(userName);
                user.setLockedTime(System.currentTimeMillis());
                user.setWrongTime(0);
            } else {
                user.setWrongTime(wrongTime + 1);
            }
        }
    }

    public static class UserObj implements UserDetails {
        private static final long serialVersionUID = 1L;

        private String username;
        private String password;
        private List<UserGrantedAuthority> authorities;
        private boolean disabled;
        private boolean defaultPassword;
        private boolean locked;
        private long lockedTime;
        private int wrongTime;

        public UserObj() {
        }

        public UserObj(String username, String password, String... authorities) {
            this.username = username;
            this.password = password;
            this.setDefaultPassword(false);
            this.setLocked(false);
            this.setLockedTime(0L);
            this.setWrongTime(0);
            this.authorities = Lists.newArrayList();

            for (String a : authorities) {
                this.authorities.add(new UserGrantedAuthority(a));
            }
        }

        public UserObj(String username, String password, Boolean defaultPassword, String... authorities) {
            this.username = username;
            this.password = password;
            this.defaultPassword = defaultPassword;
            this.setLocked(false);
            this.setLockedTime(0L);
            this.setWrongTime(0);
            this.authorities = Lists.newArrayList();

            for (String a : authorities) {
                this.authorities.add(new UserGrantedAuthority(a));
            }
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String userName) {
            this.username = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<UserGrantedAuthority> getAuthorities() {
            return authorities;
        }

        public void setAuthorities(List<UserGrantedAuthority> authorities) {
            this.authorities = authorities;
        }

        public boolean isDisabled() {
            return disabled;
        }

        public void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }

        public boolean isDefaultPassword() {
            return defaultPassword;
        }

        public void setDefaultPassword(boolean defaultPassword) {
            this.defaultPassword = defaultPassword;
        }

        public boolean isLocked() {
            return locked;
        }

        public void setLocked(boolean locked) {
            this.locked = locked;
        }

        public int getWrongTime() {
            return wrongTime;
        }

        public void setWrongTime(int wrongTime) {
            this.wrongTime = wrongTime;
        }

        public long getLockedTime() {
            return lockedTime;
        }

        public void setLockedTime(long lockedTime) {
            this.lockedTime = lockedTime;
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
            UserObj other = (UserObj) obj;
            if (username == null) {
                if (other.username != null)
                    return false;
            } else if (!username.equals(other.username))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "UserObj [username=" + username + ", authorities=" + authorities + "]";
        }
    }
}
