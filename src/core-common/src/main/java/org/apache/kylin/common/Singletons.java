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
package org.apache.kylin.common;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;

import io.kyligence.kap.common.util.Unsafe;
import lombok.val;

public class Singletons implements Serializable {

    transient ConcurrentHashMap<Class<?>, Object> instances = null;
    transient ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, Object>> instancesByPrj = null;

    public static <T> T getInstance(String project, Class<T> clz) {
        return instance.getInstance0(project, clz, defaultCreator(project));
    }

    public static <T> T getInstance(Class<T> clz) {
        return instance.getInstance0(clz, defaultCreator());
    }

    public static <T> T getInstance(String project, Class<T> clz, Creator<T> creator) {
        return instance.getInstance0(project, clz, creator);
    }

    public static <T> T getInstance(Class<T> clz, Creator<T> creator) {
        return instance.getInstance0(clz, creator);
    }

    static <T> Creator<T> defaultCreator(String project) {
        return clz -> {
            Constructor<T> method = clz.getDeclaredConstructor(String.class);
            Unsafe.changeAccessibleObject(method, true);
            return method.newInstance(project);
        };
    }

    static <T> Creator<T> defaultCreator() {
        return clz -> {
            Constructor<T> method = clz.getDeclaredConstructor();
            Unsafe.changeAccessibleObject(method, true);
            return method.newInstance();
        };
    }

    private static final Singletons instance = new Singletons();

    Singletons() {
    }

    <T> T getInstance0(Class<T> clz, Creator<T> creator) {
        Object singleton = instances == null ? null : instances.get(clz);
        if (singleton != null)
            return (T) singleton;

        synchronized (this) {
            if (instances == null)
                instances = new ConcurrentHashMap<>();

            singleton = instances.get(clz);
            if (singleton != null)
                return (T) singleton;

            try {
                singleton = creator.create(clz);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (singleton != null) {
                instances.put(clz, singleton);
            }
        }
        return (T) singleton;
    }

    <T> T getInstance0(String project, Class<T> clz, Creator<T> creator) {
        ConcurrentHashMap<String, Object> instanceMap = (null == instancesByPrj) ? null : instancesByPrj.get(clz);
        Object singleton = (null == instanceMap) ? null : instanceMap.get(project);
        if (singleton != null)
            return (T) singleton;

        synchronized (this) {
            if (null == instancesByPrj)
                instancesByPrj = new ConcurrentHashMap<>();

            instanceMap = instancesByPrj.get(clz);
            if (instanceMap == null)
                instanceMap = new ConcurrentHashMap<>();

            singleton = instanceMap.get(project);
            if (singleton != null)
                return (T) singleton;

            try {
                singleton = creator.create(clz);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (singleton != null) {
                instanceMap.put(project, singleton);
            }
            instancesByPrj.put(clz, instanceMap);
        }
        return (T) singleton;
    }

    void clear() {
        if (instances != null)
            instances.clear();
    }

    void clearByProject(String project) {
        if (instancesByPrj != null) {
            for (val value : instancesByPrj.values()) {
                value.remove(project);
            }
        }
    }

    void clearByType(Class clz) {
        if (instances != null)
            instances.remove(clz);
    }

    public interface Creator<T> {
        T create(Class<T> clz) throws Exception;
    }
}
