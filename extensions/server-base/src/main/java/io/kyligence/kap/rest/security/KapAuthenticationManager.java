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

import io.kyligence.kap.rest.controller.KapUserController.UserObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
