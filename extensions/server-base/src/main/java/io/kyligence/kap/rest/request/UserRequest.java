package io.kyligence.kap.rest.request;

import java.io.Serializable;
import java.util.List;

/**
 * Created by zhongjian on 7/1/16.
 */
public class UserRequest implements Serializable {
    private String username;
    private String password;
    private String newPassword;
    private List<String> authorities;
    private boolean disabled;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    public List<String> getAuthorities() {
        return authorities;
    }

    public void setAuthorities(List<String> authorities) {
        this.authorities = authorities;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }
}
