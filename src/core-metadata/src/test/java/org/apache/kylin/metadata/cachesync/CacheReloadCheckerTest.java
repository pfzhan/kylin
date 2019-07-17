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

package org.apache.kylin.metadata.cachesync;

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class CacheReloadCheckerTest {

    private String resPath = "/mock/mock.json";

    private String depPath1 = "/mock/mock/dep1.json";
    private String depPath2 = "/mock/mock/dep2.json";
    private String depPath3 = "/mock/mock/dep3.json";

    @Test
    public void test() {
        ResourceStore store = Mockito.mock(ResourceStore.class);
        CachedCrudAssist<RootPersistentEntity> crud = Mockito.mock(CachedCrudAssist.class);
        Cache<String, RootPersistentEntity> cache = Mockito.mock(Cache.class);

        Mockito.when(crud.getCache()).thenReturn(cache);
        CacheReloadChecker checker = new CacheReloadChecker(store, crud);

        List<RootPersistentEntity> dependencies = mockDependencies();

        RootPersistentEntity entity = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(entity.getMvcc()).thenReturn(0L);
        Mockito.when(entity.getResourcePath()).thenReturn(resPath);
        Mockito.when(entity.getDependencies()).thenReturn(dependencies);

        Mockito.when(cache.getIfPresent("mock")).thenReturn(entity);
        Mockito.when(store.getResource(resPath)).thenReturn(
                new RawResource(resPath, ByteStreams.asByteSource("version1".getBytes()), 0L, 0));
        Mockito.when(store.getResource(depPath1)).thenReturn(
                new RawResource(depPath1, ByteStreams.asByteSource("version1".getBytes()), 0L, 0));
        Mockito.when(store.getResource(depPath2)).thenReturn(
                new RawResource(depPath2, ByteStreams.asByteSource("version1".getBytes()), 0L, 0));
        Mockito.when(store.getResource(depPath3)).thenReturn(
                new RawResource(depPath3, ByteStreams.asByteSource("version1".getBytes()), 0L, 0));

        Assert.assertFalse(checker.needReload("mock"));
        Mockito.when(store.getResource(depPath3)).thenReturn(null);
        Assert.assertTrue(checker.needReload("mock"));

        dependencies.remove(2);
        dependencies.add(new MissingRootPersistentEntity(depPath3));
        Assert.assertFalse(checker.needReload("mock"));

        Mockito.when(store.getResource(depPath3)).thenReturn(
                new RawResource(depPath3, ByteStreams.asByteSource("version1".getBytes()), 0L, 0));
        Assert.assertTrue(checker.needReload("mock"));
    }

    private List<RootPersistentEntity> mockDependencies() {
        List<RootPersistentEntity> lists = Lists.newArrayList();
        RootPersistentEntity dep1 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep1.getDependencies()).thenReturn(null);
        Mockito.when(dep1.getResourcePath()).thenReturn(depPath1);
        Mockito.when(dep1.getMvcc()).thenReturn(0L);
        RootPersistentEntity dep2 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep2.getDependencies()).thenReturn(null);
        Mockito.when(dep2.getResourcePath()).thenReturn(depPath2);
        Mockito.when(dep2.getMvcc()).thenReturn(0L);
        RootPersistentEntity dep3 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep3.getDependencies()).thenReturn(null);
        Mockito.when(dep3.getResourcePath()).thenReturn(depPath3);
        Mockito.when(dep3.getMvcc()).thenReturn(0L);
        lists.add(dep1);
        lists.add(dep2);
        lists.add(dep3);
        return lists;
    }



}
