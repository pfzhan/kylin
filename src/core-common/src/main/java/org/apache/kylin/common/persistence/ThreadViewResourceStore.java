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

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FilePathUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;

import lombok.Getter;
import lombok.val;

/**
 * for meta mutation threads, it needs an exclusive view of the meta store,
 * which is underlying meta store + mutations by itself
 */
public class ThreadViewResourceStore extends ResourceStore {

    private InMemResourceStore underlying;

    @Getter
    private InMemResourceStore overlay;

    @Getter
    private List<RawResource> resources;

    public ThreadViewResourceStore(InMemResourceStore underlying, KylinConfig kylinConfig) {
        super(kylinConfig);
        this.underlying = underlying;
        this.overlay = new InMemResourceStore(kylinConfig);
        resources = Lists.newArrayList();
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) {
        //a folder named /folder with a resource named /folderxx
        folderPath = FilePathUtil.completeFolderPathWithSlash(folderPath);

        NavigableSet<String> fromUnderlying = underlying.listResourcesImpl(folderPath, true);
        NavigableSet<String> fromOverlay = overlay.listResourcesImpl(folderPath, true);
        TreeSet<String> ret = new TreeSet<>();
        if (fromUnderlying != null)
            ret.addAll(fromUnderlying);
        if (fromOverlay != null)
            ret.addAll(fromOverlay);
        ret.removeIf(key -> overlay.getResourceImpl(key) == TombRawResource.getINSTANCE());

        String finalFolderPath = folderPath;

        if (ret.isEmpty()) {
            return null;
        } else if (recursive) {
            return ret;
        } else {
            TreeSet<String> ret2 = new TreeSet<>();

            ret.stream().map(x -> {
                if (recursive) {
                    return x;
                }
                return InMemResourceStore.mapToFolder(x, recursive, finalFolderPath);
            }).forEach(ret2::add);
            return ret2;
        }

    }

    @Override
    protected boolean existsImpl(String resPath) {
        RawResource overlayResource = overlay.getResourceImpl(resPath);
        if (overlayResource != null) {
            return overlayResource != TombRawResource.getINSTANCE();
        }

        return underlying.exists(resPath);
    }

    @Override
    protected RawResource getResourceImpl(String resPath) {
        val r = overlay.getResourceImpl(resPath);
        if (r != null) {
            return r == TombRawResource.getINSTANCE() ? null //deleted
                    : r; // updated
        }

        return underlying.getResourceImpl(resPath);

    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        RawResource r = overlay.getResourceImpl(resPath);

        if (r == null) {
            //TODO: should check meta's write footprint
            long resourceMvcc = underlying.getResourceMvcc(resPath);
            Preconditions.checkState(resourceMvcc == oldMvcc, "Resource mvcc not equals old mvcc", resourceMvcc, oldMvcc);
            overlay.putResourceWithoutCheck(resPath, byteSource, System.currentTimeMillis(), oldMvcc + 1);
        } else {
            if (!KylinConfig.getInstanceFromEnv().isUTEnv() && r instanceof TombRawResource) {
                    throw new IllegalStateException(String.format("It's not allowed to create the same metadata in path {%s} after deleting it in one transaction", resPath));
            }
            overlay.checkAndPutResource(resPath, byteSource, oldMvcc);
        }

        val raw = overlay.getResourceImpl(resPath);
        resources.add(raw);
        return raw;
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        overlay.putTomb(resPath);
        resources.add(new TombRawResource(resPath));
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return toString() + ":" + resPath;
    }

    @Override
    public String toString() {
        return "<thread view metastore@" + System.identityHashCode(this) + ":KylinConfig@"
                + System.identityHashCode(kylinConfig.base()) + ">";
    }

}
