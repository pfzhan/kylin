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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.user;

import static org.apache.kylin.common.persistence.ResourceStore.USER_ROOT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;

public class NKylinUserManager {

    private static final Logger logger = LoggerFactory.getLogger(NKylinUserManager.class);

    public static NKylinUserManager getInstance(KylinConfig config) {
        return config.getManager(NKylinUserManager.class);
    }

    // called by reflection
    static NKylinUserManager newInstance(KylinConfig config) throws IOException {
        return new NKylinUserManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> ManagedUser
    private CachedCrudAssist<ManagedUser> crud;

    public NKylinUserManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NKylinUserManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<ManagedUser>(getStore(), USER_ROOT, "", ManagedUser.class) {
            @Override
            protected ManagedUser initEntityAfterReload(ManagedUser user, String resourceName) {
                return user;
            }
        };

        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public ManagedUser get(String name) {
        return crud.get(name);
    }

    public List<ManagedUser> list() {
        List<ManagedUser> users = new ArrayList<>(crud.listAll());
        users.sort((o1, o2) -> o1.getUsername().compareToIgnoreCase(o2.getUsername()));
        return users;
    }

    public void update(ManagedUser user) {
        ManagedUser exist = crud.get(user.getUsername());
        if (exist != null) {
            user.setLastModified(exist.getLastModified());
            user.setMvcc(exist.getMvcc());
        }
        crud.save(user);
    }

    public void delete(String username) {
        crud.delete(username);
    }

    public boolean exists(String username) {
        return crud.contains(username);
    }

    public Set<String> getUserGroups(String userName) {
        ManagedUser user = get(userName);
        if (user == null)
            return Sets.newHashSet();

        return user.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority).collect(Collectors.toSet());
    }
}
