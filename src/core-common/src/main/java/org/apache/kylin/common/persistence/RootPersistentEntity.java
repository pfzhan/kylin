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

package org.apache.kylin.common.persistence;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

import lombok.Setter;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.kylin.common.KylinVersion;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Marks the root entity of JSON persistence. Unit of read, write, cache, and
 * refresh.
 * <p>
 * - CubeInstance - CubeDesc - SourceTable - JobMeta - Dictionary (not JSON but
 * also top level persistence)
 *
 * @author yangli9
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
@Slf4j
abstract public class RootPersistentEntity implements AclEntity, Serializable {

    static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss z";
    static FastDateFormat format = FastDateFormat.getInstance(DATE_PATTERN);
    static DateFormat df = new SimpleDateFormat(DATE_PATTERN);

    public static String formatTime(long millis) {
        return format.format(millis);
    }

    // ============================================================================

    @JsonProperty("uuid")
    protected String uuid = UUID.randomUUID().toString();

    @JsonProperty("last_modified")
    protected long lastModified;

    @Getter
    @Setter
    @JsonProperty("create_time")
    protected long createTime = System.currentTimeMillis();

    // if cached and shared, the object MUST NOT be modified (call setXXX() for example)
    protected boolean isCachedAndShared = false;

    /**
     * Metadata model version
     * <p>
     * User info only, we don't do version control
     */
    @JsonProperty("version")
    protected String version = KylinVersion.getCurrentVersion().toString();

    @Getter
    @JsonProperty("mvcc")
    private long mvcc = -1;

    @Getter
    @Setter
    private boolean isBroken = false;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        checkIsNotCachedAndShared();
        this.version = version;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        checkIsNotCachedAndShared();
        this.uuid = uuid;
    }

    public String getId() {
        return uuid;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        //checkIsNotCachedAndShared(); // comment out due to let pass legacy tests, like StreamingManagerTest
        this.lastModified = lastModified;
    }

    public void updateRandomUuid() {
        setUuid(UUID.randomUUID().toString());
    }

    public boolean isCachedAndShared() {
        return isCachedAndShared;
    }

    public void setCachedAndShared(boolean isCachedAndShared) {
        if (this.isCachedAndShared && isCachedAndShared == false)
            throw new IllegalStateException();

        this.isCachedAndShared = isCachedAndShared;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared)
            throw new IllegalStateException();
    }

    public void setMvcc(long mvcc) {
        if (isCachedAndShared) {
            log.warn("cannot update mvcc for {}, from {} to {}", this.getClass(), this.mvcc, mvcc);
            log.warn("stack trace", new IllegalStateException("illegal operation"));
        }
        this.mvcc = mvcc;
    }

    /**
     * The name as a part of the resource path used to save the entity.
     * <p>
     * E.g. /resource-root-dir/{RESOURCE_NAME}.json
     */
    public String resourceName() {
        return uuid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (lastModified ^ (lastModified >>> 32));
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
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
        RootPersistentEntity other = (RootPersistentEntity) obj;
        if (lastModified != other.lastModified || !(version == null || version.equals(other.getVersion())))
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        return true;
    }

    public String getResourcePath() {
        return "";
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + uuid;
    }
}