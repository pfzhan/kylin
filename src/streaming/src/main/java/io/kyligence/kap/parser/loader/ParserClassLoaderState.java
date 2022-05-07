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

package io.kyligence.kap.parser.loader;

import java.security.AccessController;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * A project a singleton object
 * Manage classloader of resolver in project
 */
@Slf4j
public class ParserClassLoaderState {

    private final String project;

    private Set<String> loadedJars = Sets.newCopyOnWriteArraySet();

    private ClassLoader classLoader;

    private static Map<String, ParserClassLoaderState> instanceMap = Maps.newConcurrentMap();

    private ParserClassLoaderState(String project) {
        this.project = project;
        // init class loader
        ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
        AddToClassPathAction action = new AddToClassPathAction(parentLoader, Collections.emptyList(), true);
        final ParserClassLoader parserClassLoader = AccessController.doPrivileged(action);
        setClassLoader(parserClassLoader);
    }

    public static ParserClassLoaderState getInstance(String project) {
        if (instanceMap.getOrDefault(project, null) == null) {
            synchronized (ParserClassLoaderState.class) {
                if (instanceMap.getOrDefault(project, null) == null) {
                    instanceMap.put(project, new ParserClassLoaderState(project));
                }
            }
        }
        return instanceMap.get(project);
    }

    /**
     * register new jars
     * Throw an exception if it contains a registered jar
     */
    public void registerJars(Set<String> newJars) {
        try {
            if (ClassLoaderUtilities.judgeIntersection(loadedJars, newJars)) {
                throw new IllegalArgumentException("There is already a jar to load " + newJars
                        + ", please ensure that the jar will not be loaded twice");
            }
            AddToClassPathAction addAction = new AddToClassPathAction(getClassLoader(), newJars);
            final ParserClassLoader parserClassLoader = AccessController.doPrivileged(addAction);
            loadedJars.addAll(newJars);
            setClassLoader(parserClassLoader);
            log.info("Load Jars: {}", newJars);
        } catch (Exception e) {
            loadedJars.removeAll(newJars);
            throw new IllegalArgumentException("Unable to register: " + newJars, e);
        }
    }

    /**
     * Unregister jars
     */
    public void unregisterJar(Set<String> jarsToUnregister) {
        try {
            ClassLoaderUtilities.removeFromClassPath(project, jarsToUnregister.toArray(new String[0]), getClassLoader());
            loadedJars.removeAll(jarsToUnregister);
            log.info("Unload Jars: {}", jarsToUnregister);
        } catch (Exception e) {
            log.error("Unable to unregister {}", jarsToUnregister, e);
            throw new IllegalArgumentException("Unable to unregister: " + jarsToUnregister, e);
        }
    }

    public Set<String> getLoadedJars() {
        return loadedJars;
    }

    public void setLoadedJars(Set<String> loadedJars) {
        this.loadedJars = loadedJars;
    }

    public synchronized ClassLoader getClassLoader() {
        return classLoader;
    }

    public synchronized void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

}
