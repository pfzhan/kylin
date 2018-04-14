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

package org.apache.kylin.cube.upgrade.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

/**
 * After upgrade a metadata store to current version,
 * we'll label every RootPersistentEntity's version as KylinVersion.getCurrentVersion()
 */
public class MetadataVersionRefresher {

    private static final Logger logger = LoggerFactory.getLogger(MetadataVersionRefresher.class);

    private ResourceStore store;
    private String version;

    public MetadataVersionRefresher(ResourceStore resourceStore, String version) {
        this.store = resourceStore;
        this.version = version;

    }

    public void refresh() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        List<String> all = Lists.newArrayList();
        collectFiles(this.store, "/", all);

        for (String path : all) {
            if (path.endsWith(MetadataConstants.FILE_SURFIX) && !(path.startsWith(ResourceStore.DICT_RESOURCE_ROOT) || path.startsWith(ResourceStore.SNAPSHOT_RESOURCE_ROOT))) {
                logger.info("Updating metadata version of path {}", path);
                ObjectNode objectNode = (ObjectNode) mapper.readTree(this.store.getResource(path).inputStream);
                objectNode.put("version", version);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                mapper.writeValue(baos, objectNode);
                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                this.store.putResource(path, bais, System.currentTimeMillis());
            }
        }
    }

    public static void collectFiles(ResourceStore src, String path, List<String> ret) throws IOException {
        NavigableSet<String> children = src.listResources(path);

        if (children == null) {
            // case of resource (not a folder)

            ret.add(path);
        } else {
            // case of folder

            for (String child : children) {
                collectFiles(src, child, ret);
            }
        }
    }
}
