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

package org.apache.kylin.rest.security;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.PermissionGrantingStrategy;

import lombok.val;

/**
 */
public class ACLManager {

    private static final Logger logger = LoggerFactory.getLogger(ACLManager.class);

    public static ACLManager getInstance(KylinConfig config) {
        return config.getManager(ACLManager.class);
    }

    // called by reflection
    static ACLManager newInstance(KylinConfig config) {
        return new ACLManager(config);
    }

    public static final String DIR_PREFIX = "/acl/";

    // ============================================================================

    private KylinConfig config;
    private CachedCrudAssist<AclRecord> crud;

    public ACLManager(KylinConfig config) {
        this.config = config;
        ResourceStore aclStore = ResourceStore.getKylinMetaStore(config);
        this.crud = new CachedCrudAssist<AclRecord>(aclStore, "/acl", "", AclRecord.class) {
            @Override
            protected AclRecord initEntityAfterReload(AclRecord acl, String resourceName) {
                val aclPermissionFactory = SpringContext.getBean(PermissionFactory.class);
                val permissionGrantingStrategy = SpringContext.getBean(PermissionGrantingStrategy.class);
                acl.init(null, aclPermissionFactory, permissionGrantingStrategy);
                return acl;
            }
        };
        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public List<AclRecord> listAll() {
        return crud.listAll();
    }

    public void save(AclRecord record) {
        crud.save(record);
    }

    public void delete(String id) {
        crud.delete(id);
    }

    public AclRecord get(String id) {
        return crud.get(id);
    }
}