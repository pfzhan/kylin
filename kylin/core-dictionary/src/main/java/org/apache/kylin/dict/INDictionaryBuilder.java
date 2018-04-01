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

package org.apache.kylin.dict;

import java.io.IOException;

import org.apache.kylin.common.util.Dictionary;

/**
 * An once-only builder for dictionary.
 */
public interface INDictionaryBuilder {

    /** Sets the dictionary info for the dictionary being built. Mainly for GlobalDictionaryBuilder. */
    void init(NDictionaryInfo info, int baseId) throws IOException;
    
    /** Add a new value into dictionary, returns it is accepted (not null) or not. */
    boolean addValue(String value);
    
    /** Build the dictionary */
    Dictionary<String> build() throws IOException;
}
