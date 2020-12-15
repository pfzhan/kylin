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

package org.apache.kylin.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

/**
 * The object form of metadata/storage URL: IDENTIFIER@SCHEME[,PARAM=VALUE,PARAM=VALUE...]
 *  
 * It is not standard URL, but a string of specific format that shares some similar parts with URL.
 * 
 * Immutable by design.
 */
public class StorageURL {

    public static final int METADATA_MAX_LENGTH = 33;

    private static final LoadingCache<String, StorageURL> cache = CacheBuilder.newBuilder()//
            .maximumSize(100)//
            .build(new CacheLoader<String, StorageURL>() {
                @Override
                public StorageURL load(String metadataUrl) throws Exception {
                    return new StorageURL(metadataUrl);
                }
            });

    public static StorageURL valueOf(String metadataUrl) {
        try {
            return cache.get(metadataUrl);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    final String identifier;
    final String scheme;
    final Map<String, String> params;

    // package private for test
    StorageURL(String metadataUrl) {
        boolean first = true;
        String n = null;
        String s = null;
        Map<String, String> m = new LinkedHashMap<>();

        // split by comma but ignoring commas in quotes
        // see https://stackoverflow.com/a/1757107
        for (String split : metadataUrl.split(",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")) {
            if (first) {
                // identifier @ scheme
                int cut = split.lastIndexOf('@');
                if (cut < 0) {
                    n = split.trim();
                    s = "";
                } else {
                    n = split.substring(0, cut).trim();
                    s = split.substring(cut + 1).trim();
                }
                first = false;
            } else {
                // param = value
                int cut = split.indexOf('=');
                String k, v;
                if (cut < 0) {
                    k = split.trim();
                    v = "";
                } else {
                    k = split.substring(0, cut).trim();
                    v = split.substring(cut + 1).trim();
                }
                m.put(k, StringUtils.strip(v, "\"'"));
            }
        }

        this.identifier = StringUtils.isEmpty(n) ? "kylin_metadata" : n;
        this.scheme = s;
        this.params = ImmutableMap.copyOf(m);
    }

    public StorageURL(String identifier, String scheme, Map<String, String> params) {
        this.identifier = identifier;
        this.scheme = scheme;
        this.params = ImmutableMap.copyOf(params);
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getScheme() {
        return scheme;
    }

    public boolean containsParameter(String k) {
        return params.containsKey(k);
    }

    public String getParameter(String k) {
        return params.get(k);
    }

    public Map<String, String> getAllParameters() {
        return params;
    }

    public StorageURL copy(Map<String, String> params) {
        return new StorageURL(identifier, scheme, params);
    }

    public static String replaceUrl(StorageURL storageURL) {
        return storageURL.getIdentifier().replaceAll("[^0-9|a-z|A-Z|_]{1,}", "_");
    }

    public boolean metadataLengthIllegal() {
        return this.getIdentifier().length() > METADATA_MAX_LENGTH;
    }

    @Override
    public String toString() {
        String str = identifier;
        if (!scheme.isEmpty())
            str += "@" + scheme;

        for (Entry<String, String> kv : params.entrySet()) {
            str += "," + kv.getKey();
            if (!kv.getValue().isEmpty())
                str += "=" + kv.getValue();
        }
        return str;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        result = prime * result + ((scheme == null) ? 0 : scheme.hashCode());
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
        StorageURL other = (StorageURL) obj;
        if (identifier == null) {
            if (other.identifier != null)
                return false;
        } else if (!identifier.equals(other.identifier))
            return false;
        if (params == null) {
            if (other.params != null)
                return false;
        } else if (!params.equals(other.params))
            return false;
        if (scheme == null) {
            if (other.scheme != null)
                return false;
        } else if (!scheme.equals(other.scheme))
            return false;
        return true;
    }

}
