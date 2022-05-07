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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.springframework.util.ReflectionUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassLoaderUtilities {

    private ClassLoaderUtilities() {
    }

    public static boolean judgeIntersection(Set<String> set1, Collection<String> collection) {
        Set<String> setCopy = new HashSet<>(set1);
        setCopy.retainAll(collection);
        return !setCopy.isEmpty();
    }

    /**
     * Create a URL from a string representing a path to a local file.
     * The path string can be just a path, or can start with file:/、hdfs:/
     */
    public static URL urlFromPathString(String path) {
        URL resultUrl = null;
        try {
            if (StringUtils.indexOf(path, "file:/") == 0) {
                resultUrl = new URL(path);
            } else if (StringUtils.indexOf(path, "hdfs:/") == 0) {
                registerFactory(new FsUrlStreamHandlerFactory());
                resultUrl = new org.apache.hadoop.fs.Path(path).toUri().toURL();
            }
        } catch (Exception e) {
            log.error("Bad URL {}, ignoring path", path, e);
            throw new IllegalArgumentException("Bad URL [" + path + "], ignoring path");
        }
        if (resultUrl == null) {
            throw new IllegalArgumentException(
                    "URL [" + path + "] not supported, currently only HDFS and file is supported");
        }
        return resultUrl;
    }

    /**
     * URL add hdfs protocol
     */
    @SneakyThrows
    private static void registerFactory(final FsUrlStreamHandlerFactory fsUrlStreamHandlerFactory) {
        log.info("registerFactory : " + fsUrlStreamHandlerFactory.getClass().getName());
        final Field factoryField = ReflectionUtils.findField(URL.class, "factory");
        ReflectionUtils.makeAccessible(Objects.requireNonNull(factoryField));
        final Field lockField = ReflectionUtils.findField(URL.class, "streamHandlerLock");
        ReflectionUtils.makeAccessible(Objects.requireNonNull(lockField));
        // use same lock as in java.net.URL.setURLStreamHandlerFactory
        synchronized (lockField.get(null)) {
            final URLStreamHandlerFactory originalUrlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField
                    .get(null);
            // Reset the value to prevent Error due to a factory already defined
            ReflectionUtils.setField(factoryField, null, null);
            URL.setURLStreamHandlerFactory(protocol -> {
                if ("hdfs".equals(protocol)) {
                    return fsUrlStreamHandlerFactory.createURLStreamHandler(protocol);
                }
                return originalUrlStreamHandlerFactory.createURLStreamHandler(protocol);
            });
        }
    }

    public static void closeClassLoader(ClassLoader loader) throws IOException {
        if (loader instanceof Closeable) {
            ((Closeable) loader).close();
            return;
        }
        log.warn("Ignoring attempt to close class loader ({}) -- not instance of ParserClassLoader.",
                loader == null ? "mull" : loader.getClass().getSimpleName());
    }

    public static void removeFromClassPath(String project, String[] pathsToRemove, ClassLoader classLoader)
            throws IOException {

        if (!(classLoader instanceof ParserClassLoader)) {
            log.warn(
                    "Ignoring attempt to manipulate {}; probably means we have closed more Parser loaders than opened.",
                    classLoader == null ? "null" : classLoader.getClass().getSimpleName());
            return;
        }

        ParserClassLoader loader = (ParserClassLoader) classLoader;
        List<URL> newPaths = new ArrayList<>(Arrays.asList(loader.getURLs()));
        Arrays.stream(pathsToRemove).forEach(path -> newPaths.remove(urlFromPathString(path)));

        // close old ClassLoader
        closeClassLoader(loader);
        // reload new ClassLoader
        AddToClassPathAction action = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(),
                newPaths.stream().map(URL::toString).collect(Collectors.toSet()));
        final ParserClassLoader parserClassLoader = AccessController.doPrivileged(action);
        // set new ClassLoader
        ParserClassLoaderState.getInstance(project).setClassLoader(parserClassLoader);
    }
}