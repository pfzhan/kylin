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

import com.google.common.collect.ImmutableMap;

/**
 * Able to delegate a few FIRST LEVEL directories to other resource stores.
 */
public class FederatedResourceStore extends ResourceStore {
    
    final ResourceStore base;
    final Map<String, ResourceStore> delegates;
    
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

    private boolean isRoot(String normPath) {
        return "/".equals(normPath);
    }
    
    private ResourceStore select(String nonRootPath) {
        // cut first level path
        String firstLevel;
        int cut = nonRootPath.indexOf("/", 1);
        if (cut < 0) {
            firstLevel = nonRootPath;
        } else {
            firstLevel = nonRootPath.substring(0, cut);
        }
        
        // decide delegate based on first level path
        ResourceStore r = delegates.get(firstLevel);
        return r == null ? base : r;
    }
    
    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        if (isRoot(folderPath)) {
            NavigableSet<String> result = base.listResourcesImpl("/");
            for (String firstLevel : delegates.keySet()) {
                result.remove(firstLevel); // ensures delegated content only comes from delegated store
                NavigableSet<String> subResult = delegates.get(firstLevel).listResourcesImpl("/");
                if (subResult.contains(firstLevel))
                    result.add(firstLevel);
            }
            return result;
        } else {
            return select(folderPath).listResourcesImpl(folderPath);
        }
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        if (isRoot(resPath))
            return false;
        else
            return select(resPath).existsImpl(resPath);
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive)
            throws IOException {
        if (isRoot(folderPath))
            throw new IllegalArgumentException();
            
        return select(folderPath).getAllResourcesImpl(folderPath, timeStart, timeEndExclusive);
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        if (isRoot(resPath))
            return null;
        else
            return select(resPath).getResourceImpl(resPath);
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        if (isRoot(resPath))
            return 0;
        else
            return select(resPath).getResourceTimestampImpl(resPath);
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        if (isRoot(resPath))
            throw new IllegalArgumentException();
        
        select(resPath).putResourceImpl(resPath, content, ts);
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, IllegalStateException {
        if (isRoot(resPath))
            throw new IllegalArgumentException();
        
        return select(resPath).checkAndPutResourceImpl(resPath, content, oldTS, newTS);
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        if (isRoot(resPath))
            throw new IllegalArgumentException();
        
        select(resPath).deleteResourceImpl(resPath);
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return "FederatedResourceStore [base=" + base + ", delegates=" + delegates + "]";
    }
}
