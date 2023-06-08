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

package org.apache.kylin.parser;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

@Accessors(chain = true)
public class ParserConfig implements Serializable {

    @Getter
    boolean caseSensitive = false;

    @Getter
    private Set<String> includes;

    public ParserConfig() {
        this(false);
    }

    public ParserConfig(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        if (caseSensitive) {
            this.includes = Sets.newHashSet();
        } else {
            this.includes = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        }
    }

    public ParserConfig setIncludes(Collection<String> keys) {
        includes.clear();
        includes.addAll(keys);
        return this;
    }

    public ParserConfig copy() {
        return new ParserConfig(caseSensitive).setIncludes(includes);
    }
}
