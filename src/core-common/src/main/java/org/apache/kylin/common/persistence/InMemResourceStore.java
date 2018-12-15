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

package org.apache.kylin.common.persistence;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FilePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteSource;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemResourceStore.class);

    @Getter
    private final ConcurrentSkipListMap<String, VersionedRawResource> data = new ConcurrentSkipListMap<>();

    public InMemResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) {
        //a folder named /folder with a resource named /folderxx
        folderPath = FilePathUtil.completeFolderPathWithSlash(folderPath);

        val subset = data.subMap(folderPath, folderPath + Character.MAX_VALUE);

        TreeSet<String> ret = new TreeSet<>();
        String finalFolderPath = folderPath;
        subset.entrySet().stream().map(Map.Entry::getKey).map(x -> mapToFolder(x, recursive, finalFolderPath))
                .forEach(ret::add);

        // return null to indicate not a folder
        return ret.isEmpty() ? null : ret;
    }

    static String mapToFolder(String path, boolean recursive, String folderPath) {
        if (recursive)
            return path;

        int index = path.indexOf("/", folderPath.length());
        if (index >= 0)
            return path.substring(0, index);
        else
            return path;
    }

    @Override
    protected boolean existsImpl(String resPath) {
        return getResourceImpl(resPath) != null;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) {
        VersionedRawResource orDefault = data.getOrDefault(resPath, null);
        if (orDefault == null) {
            return null;
        }
        return orDefault.getRawResource();
    }

    protected void putTomb(String resPath) {
        data.put(resPath, TombVersionedRawResource.getINSTANCE());
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        checkEnv();
        if (!data.containsKey(resPath) || data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
            if (oldMvcc != -1) {
                throw new IllegalStateException(
                        "Trying to update a non-exist meta entry: " + resPath + ", with mvcc: " + oldMvcc);
            }
            synchronized (data) {
                if (!data.containsKey(resPath) || data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
                    RawResource rawResource = new RawResource(resPath, byteSource, System.currentTimeMillis(),
                            oldMvcc + 1);
                    data.put(resPath, new VersionedRawResource(rawResource));
                    return rawResource;
                }
            }
        }
        VersionedRawResource versionedRawResource = data.get(resPath);
        RawResource r = new RawResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc + 1);
        try {
            versionedRawResource.update(r);
        } catch (VersionConflictException e) {
            logger.info("current RS: {}", this.toString());
            throw e;
        }

        return r;
    }

    protected long getResourceMvcc(String resPath) {
        if (!data.containsKey(resPath)) {
            return -1;
        }

        if (data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
            //getResourceMvcc is only called on underlying
            throw new IllegalStateException();
        }

        VersionedRawResource versionedRawResource = data.get(resPath);
        return versionedRawResource.getMvcc();
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        checkEnv();
        this.data.remove(resPath);
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return toString() + ":" + resPath;
    }

    @Override
    public String toString() {
        return "<in memory metastore@" + System.identityHashCode(this) + ":kylin config@"
                + System.identityHashCode(kylinConfig.base()) + ">";
    }

    @Override
    public void putResourceWithoutCheck(String resPath, ByteSource bs, long newMvcc) {
        synchronized (data) {
            if (data.containsKey(resPath) && data.get(resPath) != TombVersionedRawResource.getINSTANCE()) {
                throw new IllegalStateException(
                        "resource " + resPath + " already exists, use check and put api instead");
            }
            RawResource rawResource = new RawResource(resPath, bs, System.currentTimeMillis(), newMvcc);
            data.put(resPath, new VersionedRawResource(rawResource));
        }
    }

    private void checkEnv() {
        // UT env or replay thread can ignore transactional lock
        if (!kylinConfig.isSystemConfig() || kylinConfig.isUTEnv() || UnitOfWork.isReplaying()) {
            return;
        }
        throw new IllegalStateException("cannot update or delete resource");
    }
}
