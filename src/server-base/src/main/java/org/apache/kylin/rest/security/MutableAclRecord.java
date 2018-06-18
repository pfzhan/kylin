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

package org.apache.kylin.rest.security;

import java.io.Serializable;
import java.util.List;

import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.OwnershipAcl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.acls.model.UnloadedSidException;

/**
 * A thin wrapper around AclRecord to work around the conflict between MutableAcl.getId() and AclEntity.getId()
 * having different return types.
 */
@SuppressWarnings("serial")
public class MutableAclRecord implements Acl, MutableAcl, OwnershipAcl {

    private final AclRecord acl;

    public MutableAclRecord(AclRecord acl) {
        this.acl = acl;
    }
    
    public AclRecord getAclRecord() {
        return acl;
    }
    
    @Override
    public Serializable getId() {
        return acl.getDomainObjectInfo().getIdentifier();
    }
    
    @Override
    public ObjectIdentity getObjectIdentity() {
        return acl.getObjectIdentity();
    }

    @Override
    public Sid getOwner() {
        return acl.getOwner();
    }

    @Override
    public Acl getParentAcl() {
        return acl.getParentAcl();
    }

    @Override
    public boolean isEntriesInheriting() {
        return acl.isEntriesInheriting();
    }

    @Override
    public void setOwner(Sid newOwner) {
        acl.setOwner(newOwner);
    }

    @Override
    public void setEntriesInheriting(boolean entriesInheriting) {
        acl.setEntriesInheriting(entriesInheriting);
    }

    @Override
    public void setParent(Acl newParent) {
        acl.setParent(newParent);
    }

    @Override
    public List<AccessControlEntry> getEntries() {
        return acl.getEntries();
    }
    
    @Override
    public void insertAce(int atIndexLocation, Permission permission, Sid sid, boolean granting)
            throws NotFoundException {
        acl.insertAce(atIndexLocation, permission, sid, granting);
    }

    @Override
    public void updateAce(int aceIndex, Permission permission) throws NotFoundException {
        acl.updateAce(aceIndex, permission);
    }

    @Override
    public void deleteAce(int aceIndex) throws NotFoundException {
        acl.deleteAce(aceIndex);
    }
    
    @Override
    public boolean isGranted(List<Permission> permission, List<Sid> sids, boolean administrativeMode)
            throws NotFoundException, UnloadedSidException {
        return acl.isGranted(permission, sids, administrativeMode);
    }

    @Override
    public boolean isSidLoaded(List<Sid> sids) {
        return acl.isSidLoaded(sids);
    }

}
