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

package io.kyligence.kap.tool.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Locale;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.google.common.base.Throwables;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;

class RenameEntity {

    private static final ResourceStore resourceStore;

    private String originName;
    private String destName;
    private RawResource rs;

    static {
        resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    /**
     * update path
     *
     * @param originName
     * @param destName
     */
    public RenameEntity(String originName, String destName) {
        this.originName = originName;
        this.destName = destName;
    }

    /**
     * update path
     *
     * @param originName
     * @param destName
     */
    public RenameEntity(String originName, String destName, RawResource rs) {
        this.originName = originName;
        this.destName = destName;
        this.rs = rs;
    }

    /**
     * update path and content
     *
     * @param originName
     * @param destName
     * @param entity
     * @param clazz
     */
    public RenameEntity(String originName, String destName, RootPersistentEntity entity, Class clazz) {
        this.originName = originName;
        this.destName = destName;

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        try {
            new JsonSerializer<>(clazz).serialize(entity, dout);
            dout.close();
            buf.close();
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

        this.rs = new RawResource(destName, byteSource, System.currentTimeMillis(), entity.getMvcc());
    }

    public RawResource getRs() {
        if (rs == null && StringUtils.isNotEmpty(originName)) {
            RawResource resource = resourceStore.getResource(originName);
            if (resource != null) {
                this.rs = new RawResource(destName, resource.getByteSource(), System.currentTimeMillis(),
                        resource.getMvcc());
            }
        }
        return rs;
    }

    public String getOriginName() {
        return originName;
    }

    public String getDestName() {
        return destName;
    }

    public void updateMetadata() throws Exception {
        MetadataStore metadataStore = resourceStore.getMetadataStore();
        RawResource rawResource = this.getRs();
        if (rawResource != null) {
            metadataStore.move(this.getOriginName(), this.getDestName());
            metadataStore.deleteResource(this.getOriginName(), null, UnitOfWork.DEFAULT_EPOCH_ID);
            metadataStore.putResource(rawResource, null, UnitOfWork.DEFAULT_EPOCH_ID);
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s -> %s", originName, destName);
    }
}
