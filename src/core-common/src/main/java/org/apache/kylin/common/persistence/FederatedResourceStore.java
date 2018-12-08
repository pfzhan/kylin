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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Able to delegate a few FIRST LEVEL directories to other resource stores.
 */
public class FederatedResourceStore extends ResourceStore {

    final ResourceStore base;
    final Map<String, ResourceStore> delegates;
    final List<String> reservedNonTopDir = ImmutableList.of(ResourceStore.METASTORE_UUID_TAG,
            ResourceStore.QUERY_HISTORY_TIME_OFFSET, ResourceStore.USER_GROUP_ROOT, ResourceStore.USER_ROOT,
            "/kylin.properties");

    public FederatedResourceStore(ResourceStore baseStore, Map<String, ResourceStore> delegates) {
        super(baseStore.kylinConfig, baseStore.storageUrl);
        this.base = baseStore;
        this.delegates = ImmutableMap.copyOf(delegates);
    }

    public ResourceStore getBase() {
        return base;
    }

    public Map<String, ResourceStore> getDelegates() {
        return delegates;
    }

    /**
     * We name directories contains both metadata directories and storage directories as TopDir
     * A TopDir should be something like: "/" or "/{project}"
     *
     * @param normPath
     * @return
     */
    private boolean isTopDir(String normPath) {
        return !reservedNonTopDir.contains(normPath) && StringUtils.countMatches(normPath, "/") == 1;
    }

    /**
     *
     * @param nonTopDir should be like /project/xxx or /{project}/xxx/yyy
     * @return
     */
    private ResourceStore select(String nonTopDir) {
        // remove the first level directory
        String category = nonTopDir;
        String[] directories = nonTopDir.split("/");
        if (directories.length > 2) {
            category = "/" + directories[2];
        }

        // decide delegate based on the category
        ResourceStore r = delegates.get(category);
        return r == null ? base : r;
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException {
        if (isTopDir(folderPath)) {
            NavigableSet<String> result = base.listResourcesImpl(folderPath, recursive);
            for (ResourceStore store : delegates.values()) {
                NavigableSet<String> subResult = store.listResourcesImpl(folderPath, recursive);
                if (null != subResult) {
                    if (null == result) {
                        result = new TreeSet<>();
                    }
                    result.addAll(subResult);
                }
            }
            return result;
        } else {
            return select(folderPath).listResourcesImpl(folderPath, recursive);
        }
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        if (isTopDir(resPath))
            return false;
        else
            return select(resPath).existsImpl(resPath);
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive)
            throws IOException {
        if (isTopDir(folderPath))
            throw new IllegalArgumentException();

        return select(folderPath).getAllResourcesImpl(folderPath, timeStart, timeEndExclusive);
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        if (isTopDir(resPath))
            return null;
        else
            return select(resPath).getResourceImpl(resPath);
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        if (isTopDir(resPath))
            return 0;
        else
            return select(resPath).getResourceTimestampImpl(resPath);
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        if (isTopDir(resPath))
            throw new IllegalArgumentException();

        select(resPath).putResourceImpl(resPath, content, ts);
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, IllegalStateException {
        if (isTopDir(resPath))
            throw new IllegalArgumentException();

        return select(resPath).checkAndPutResourceImpl(resPath, content, oldTS, newTS);
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        if (isTopDir(resPath))
            throw new IllegalArgumentException();

        select(resPath).deleteResourceImpl(resPath);
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return "FederatedResourceStore [base=" + base + ", delegates=" + delegates + "]";
    }
}
