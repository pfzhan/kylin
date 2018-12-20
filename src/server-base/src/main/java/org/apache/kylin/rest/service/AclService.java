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

package org.apache.kylin.rest.service;

import static org.apache.kylin.rest.security.ACLManager.DIR_PREFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.ACLManager;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ChildrenExistException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Component("aclService")
public class AclService implements MutableAclService {
    private static final Logger logger = LoggerFactory.getLogger(AclService.class);

    // ============================================================================

    @Autowired
    protected PermissionGrantingStrategy permissionGrantingStrategy;

    @Autowired
    protected PermissionFactory aclPermissionFactory;

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        List<ObjectIdentity> oids = new ArrayList<>();
        Collection<AclRecord> allAclRecords;
        allAclRecords = getAclManager().listAll();
        for (AclRecord record : allAclRecords) {
            ObjectIdentityImpl parent = record.getParentDomainObjectInfo();
            if (parent != null && parent.equals(parentIdentity)) {
                ObjectIdentityImpl child = record.getDomainObjectInfo();
                oids.add(child);
            }
        }
        return oids;
    }

    public MutableAclRecord readAcl(ObjectIdentity oid) throws NotFoundException {
        return (MutableAclRecord) readAclById(oid);
    }

    @Override
    public Acl readAclById(ObjectIdentity object) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), null);
        return aclsMap.get(object);
    }

    @Override
    public Acl readAclById(ObjectIdentity object, List<Sid> sids) throws NotFoundException {
        Message msg = MsgPicker.getMsg();
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), sids);
        if (!aclsMap.containsKey(object)) {
            throw new BadRequestException(String.format(msg.getNO_ACL_ENTRY(), object));
        }
        return aclsMap.get(object);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects) throws NotFoundException {
        return readAclsById(objects, null);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> oids, List<Sid> sids) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclMaps = new HashMap<>();
        for (ObjectIdentity oid : oids) {
            AclRecord record = getAclRecordByCache(objID(oid));
            if (record == null) {
                Message msg = MsgPicker.getMsg();
                throw new NotFoundException(String.format(msg.getACL_INFO_NOT_FOUND(), oid));
            }

            Acl parentAcl = null;
            if (record.isEntriesInheriting() && record.getParentDomainObjectInfo() != null)
                parentAcl = readAclById(record.getParentDomainObjectInfo());

            record.init(parentAcl, aclPermissionFactory, permissionGrantingStrategy);

            aclMaps.put(oid, new MutableAclRecord(record));
        }
        return aclMaps;
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
        AclRecord aclRecord = getAclRecordByCache(objID(objectIdentity));
        if (aclRecord != null) {
            throw new AlreadyExistsException("ACL of " + objectIdentity + " exists!");
        }
        AclRecord record = newPrjACL(objectIdentity);
        getAclManager().save(record);
        logger.debug("ACL of " + objectIdentity + " created successfully.");
        return (MutableAcl) readAclById(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
        List<ObjectIdentity> children = findChildren(objectIdentity);
        if (!deleteChildren && children.size() > 0) {
            Message msg = MsgPicker.getMsg();
            throw new BadRequestException(String.format(msg.getIDENTITY_EXIST_CHILDREN(), objectIdentity));
        }
        for (ObjectIdentity oid : children) {
            deleteAcl(oid, deleteChildren);
        }
        getAclManager().delete(objID(objectIdentity));
        logger.debug("ACL of " + objectIdentity + " deleted successfully.");
    }

    // Try use the updateAclWithRetry() method family whenever possible
    @Override
    public MutableAcl updateAcl(MutableAcl mutableAcl) throws NotFoundException {
        AclRecord record = ((MutableAclRecord) mutableAcl).getAclRecord();
        getAclManager().save(record);
        logger.debug("ACL of " + mutableAcl.getObjectIdentity() + " updated successfully.");
        return mutableAcl;
    }

    // a NULL permission means to delete the ace
    MutableAclRecord upsertAce(MutableAclRecord acl, final Sid sid, final Permission perm) {
        return updateAclWithRetry(acl, new AclRecordUpdater() {
            @Override
            public void update(AclRecord record) {
                record.upsertAce(perm, sid);
            }
        });
    }

    void batchUpsertAce(MutableAclRecord acl, final Map<Sid, Permission> sidToPerm) {
        updateAclWithRetry(acl, new AclRecordUpdater() {
            @Override
            public void update(AclRecord record) {
                for (Sid sid : sidToPerm.keySet()) {
                    record.upsertAce(sidToPerm.get(sid), sid);
                }
            }
        });
    }

    MutableAclRecord inherit(MutableAclRecord acl, final MutableAclRecord parentAcl) {
        return updateAclWithRetry(acl, new AclRecordUpdater() {
            @Override
            public void update(AclRecord record) {
                record.setEntriesInheriting(true);
                record.setParent(parentAcl);
            }
        });
    }

    @Nullable
    private AclRecord getAclRecordByCache(String id) {
        return getAclManager().get(id);
    }

    private AclRecord newPrjACL(ObjectIdentity objID) {
        AclRecord acl = new AclRecord(objID, getCurrentSid());
        acl.init(null, this.aclPermissionFactory, this.permissionGrantingStrategy);
        acl.updateRandomUuid();
        return acl;
    }

    private Sid getCurrentSid() {
        return new PrincipalSid(SecurityContextHolder.getContext().getAuthentication());
    }

    public ACLManager getAclManager() {
        return ACLManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    public interface AclRecordUpdater {
        void update(AclRecord record);
    }

    private MutableAclRecord updateAclWithRetry(MutableAclRecord acl, AclRecordUpdater updater) {
        int retry = 7;
        while (retry-- > 0) {
            AclRecord record = acl.getAclRecord();

            updater.update(record);
            try {
                getAclManager().save(record);
                return acl; // here we are done

            } catch (WriteConflictException ise) {
                if (retry <= 0) {
                    logger.error("Retry is out, till got error, abandoning...", ise);
                    throw ise;
                }

                logger.warn("Write conflict to update ACL " + resourceKey(record.getObjectIdentity())
                        + " retry remaining " + retry + ", will retry...");
                acl = readAcl(acl.getObjectIdentity());

            }
        }
        throw new RuntimeException("should not reach here");
    }

    private static String resourceKey(ObjectIdentity domainObjId) {
        return resourceKey(objID(domainObjId));
    }

    private static String objID(ObjectIdentity domainObjId) {
        return String.valueOf(domainObjId.getIdentifier());
    }

    static String resourceKey(String domainObjId) {
        return DIR_PREFIX + domainObjId;
    }
}
