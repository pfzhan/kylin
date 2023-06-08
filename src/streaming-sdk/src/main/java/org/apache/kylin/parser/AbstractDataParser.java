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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Custom parser abstract class
 * 1. Override parse(I input) method
 * 2. Initialization once is done in a parameterless construct
 * 3. Initialization every data in before() method
 * 4. Check the parsed data in after() method
 * Indicates that the current data is incorrect and needs to be skipped in the construction. Please throw an exception in the appropriate position
 */
@Slf4j
public abstract class AbstractDataParser<I, V> implements Serializable {

    public static <I, V> AbstractDataParser<I, V> getDataParser(String parserPath, ClassLoader classLoader)
            throws ReflectiveOperationException {
        if (StringUtils.isEmpty(parserPath)) {
            throw new IllegalStateException("Invalid parserName " + parserPath);
        }
        Class<?> clazz = Class.forName(parserPath, true, classLoader);
        Constructor<?> constructor = clazz.getConstructor();
        Object instance = constructor.newInstance();
        if (!(instance instanceof AbstractDataParser)) {
            throw new IllegalStateException(parserPath + " does not extends from AbstractDataParser");
        }
        return (AbstractDataParser<I, V>) instance;
    }

    protected AbstractDataParser() {
    }

    public Optional<V> process(I input) {
        before();
        if (Objects.isNull(input)) {
            log.error("input data is null ...");
            return Optional.empty();
        }
        return after(parse(input));
    }

    public void withConfig(ParserConfig config) {}

    /**
     * init something before parse one data
     */
    protected void before() {
    }

    /**
     * need to be overridden
     */
    protected abstract Optional<V> parse(I input);

    /**
     * check parsed data
     */
    protected Optional<V> after(Optional<V> parseMap) {
        return parseMap;
    }

    /**
     * Used to define data types
     */
    protected Map<String, Object> defineDataTypes() {
        return Collections.emptyMap();
    }
}
