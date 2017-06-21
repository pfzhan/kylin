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

package io.kyligence.kap.ext;

import java.util.HashSet;
import java.util.Set;

import org.apache.catalina.loader.ParallelWebappClassLoader;

/**
 * simple extension to standard ParallelWebappClassLoader
 * the only difference is that CustomizedWebappClassloader is able to delegate more packages
 * to parent classloaders
 */
public class CustomizedWebappClassloader extends ParallelWebappClassLoader {
    /**
     * Set of package names which are not allowed to be loaded from a webapp
     * class loader without delegating first.
     */
    private static final String[] packageTriggers = { "org.slf4j" };

    private static final Set<String> wontFindClasses = new HashSet<>();

    private static final String[] wontFindClassPrefixes = { "Baz" };

    static {
        wontFindClasses.add("Class");
        wontFindClasses.add("Object");
        wontFindClasses.add("org");
        wontFindClasses.add("java.lang.org");
        wontFindClasses.add("java.lang$org");
        wontFindClasses.add("java$lang$org");
        wontFindClasses.add("org.apache");
        wontFindClasses.add("org.apache.calcite");
        wontFindClasses.add("org.apache.calcite.runtime");
        wontFindClasses.add("org.apache.calcite.linq4j");
        wontFindClasses.add("Long");
        wontFindClasses.add("String");
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {

        if (wontFindClasses.contains(name)) {
            throw new ClassNotFoundException(name);
        }

        for (String s : wontFindClassPrefixes) {
            if (name.startsWith(s)) {
                throw new ClassNotFoundException(name);
            }
        }

        Class<?> aClass = super.loadClass(name, resolve);
        return aClass;
    }

    public CustomizedWebappClassloader() {
    }

    public CustomizedWebappClassloader(ClassLoader parent) {
        super(parent);
    }

    /**
     * Filter classes.
     *
     * @param name class name
     * @return true if the class should be filtered
     */
    protected boolean filter(String name) {
        if (name == null)
            return false;

        // Looking up the package
        String packageName = null;
        int pos = name.lastIndexOf('.');
        if (pos != -1)
            packageName = name.substring(0, pos);
        else
            return false;

        for (int i = 0; i < packageTriggers.length; i++) {
            if (packageName.startsWith(packageTriggers[i]))
                return true;
        }

        return super.filter(name);
    }
}
