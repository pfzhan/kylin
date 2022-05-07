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

import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;

/**
 * Reference from hive org.apache.hadoop.hive.ql.exec.AddToClassPathAction
 */
public class AddToClassPathAction implements PrivilegedAction<ParserClassLoader> {

    private final ClassLoader parentLoader;
    private final Collection<String> newPaths;
    private final boolean forceNewClassLoader;

    public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths, boolean forceNewClassLoader) {
        this.parentLoader = parentLoader;
        this.newPaths = newPaths != null ? newPaths : Collections.emptyList();
        this.forceNewClassLoader = forceNewClassLoader;
        if (parentLoader == null) {
            throw new IllegalArgumentException("ParserClassLoader is not designed to be a bootstrap class loader!");
        }
    }

    public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths) {
        this(parentLoader, newPaths, false);
    }

    @Override
    public ParserClassLoader run() {
        if (useExistingClassLoader()) {
            final ParserClassLoader parserClassLoader = (ParserClassLoader) this.parentLoader;
            for (String path : newPaths) {
                parserClassLoader.addURL(ClassLoaderUtilities.urlFromPathString(path));
            }
            return parserClassLoader;
        }
        return createParserClassLoader();
    }

    private boolean useExistingClassLoader() {
        if (!forceNewClassLoader && parentLoader instanceof ParserClassLoader) {
            final ParserClassLoader parserClassLoader = (ParserClassLoader) this.parentLoader;
            return !parserClassLoader.isClosed();
        }
        return false;
    }

    private ParserClassLoader createParserClassLoader() {
        return new ParserClassLoader(newPaths.stream().map(ClassLoaderUtilities::urlFromPathString).toArray(URL[]::new),
                parentLoader);
    }

}
