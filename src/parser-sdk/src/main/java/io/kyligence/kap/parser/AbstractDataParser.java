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

package io.kyligence.kap.parser;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Custom parser abstract class
 * Override parse(I input) method
 * Initialization once is done in a parameterless construct
 * Initialization every data in before() method
 * Check the parsed data in after(Map<String, Object> parseMap) method
 * Indicates that the current data is incorrect and needs to be skipped in the construction. Please throw an exception in the appropriate position
 */
@Slf4j
public abstract class AbstractDataParser<I> implements Serializable {

    public static <I> AbstractDataParser<I> getDataParser(String parserPath, ClassLoader classLoader)
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
        return (AbstractDataParser<I>) instance;
    }

    protected AbstractDataParser() {
    }

    public Map<String, Object> process(I input) {
        before();
        if (ObjectUtils.isEmpty(input)) {
            log.error("input data is empty ...");
            return Collections.emptyMap();
        }
        return after(parse(input));
    }

    /**
     * init something before parse one data
     */
    protected void before() {
    }

    /**
     * need to be overridden
     */
    protected abstract Map<String, Object> parse(I input);

    /**
     * check parsed data
     */
    protected Map<String, Object> after(Map<String, Object> parseMap) {
        return parseMap;
    }

    /**
     * Used to define data types
     */
    protected Map<String, Object> defineDataTypes() {
        return Collections.emptyMap();
    }

}
