/**
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

package io.kyligence.kap.cube.index.pinot.util;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.Attributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 */
public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * Rethrows an exception, even if it is not in the method signature.
     *
     * @param t The exception to rethrow.
     */
    public static void rethrowException(Throwable t) {
        /* Error can be thrown anywhere and is type erased on rethrowExceptionInner, making the cast in
        rethrowExceptionInner a no-op, allowing us to rethrow the exception without declaring it. */
        Utils.<Error> rethrowExceptionInner(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrowExceptionInner(Throwable exception) throws T {
        throw (T) exception;
    }

    /**
     * Obtains the name of the calling method and line number. This is slow, only use this for debugging!
     */
    public static String getCallingMethodDetails() {
        try {
            throw new RuntimeException();
        } catch (RuntimeException e) {
            return e.getStackTrace()[2].toString();
        }
    }

    private static final AtomicLong _uniqueIdGen = new AtomicLong(1);

    public static long getUniqueId() {
        return _uniqueIdGen.incrementAndGet();
    }

    /**
     * Takes a string, removes all characters that are not letters or digits and capitalizes the next letter following a
     * series of characters that are not letters or digits. For example, toCamelCase("Hello world!") returns "HelloWorld".
     *
     * @param text The text to camel case
     * @return The camel cased version of the string given
     */
    public static String toCamelCase(String text) {
        int length = text.length();
        StringBuilder builder = new StringBuilder(length);

        boolean capitalizeNextChar = false;

        for (int i = 0; i < length; i++) {
            char theChar = text.charAt(i);
            if (Character.isLetterOrDigit(theChar) || theChar == '.') {
                if (capitalizeNextChar) {
                    builder.append(Character.toUpperCase(theChar));
                    capitalizeNextChar = false;
                } else {
                    builder.append(theChar);
                }
            } else {
                capitalizeNextChar = true;
            }
        }

        return builder.toString();
    }

    /**
     * Write the version of Pinot components to the log at info level.
     */
    public static void logVersions() {
        for (Map.Entry<String, String> titleVersionEntry : getComponentVersions().entrySet()) {
            LOGGER.info("Using {} {}", titleVersionEntry.getKey(), titleVersionEntry.getValue());
        }
    }

    /**
     * Obtains the version numbers of the Pinot components.
     *
     * @return A map of component name to component version.
     */
    public static Map<String, String> getComponentVersions() {
        Map<String, String> componentVersions = new HashMap<>();

        // Find the first URLClassLoader, walking up the chain of parent classloaders if necessary
        ClassLoader classLoader = Utils.class.getClassLoader();
        while (classLoader != null && !(classLoader instanceof URLClassLoader)) {
            classLoader = classLoader.getParent();
        }

        if (classLoader != null) {
            URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            URL[] urls = urlClassLoader.getURLs();
            for (URL url : urls) {
                try {
                    // Convert the URL to the JAR into a JAR URL: eg. jar:http://www.foo.com/bar/baz.jar!/ in order to load it
                    URL jarUrl = new URL("jar", "", url + "!/");
                    URLConnection connection = jarUrl.openConnection();
                    if (connection instanceof JarURLConnection) {
                        JarURLConnection jarURLConnection = (JarURLConnection) connection;

                        // Read JAR attributes and log the Implementation-Title and Implementation-Version manifestvalues for pinot
                        // components
                        Attributes attributes = jarURLConnection.getMainAttributes();
                        if (attributes != null) {
                            String implementationTitle = attributes.getValue(Attributes.Name.IMPLEMENTATION_TITLE);
                            String implementationVersion = attributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
                            if (implementationTitle != null && implementationTitle.contains("pinot")) {
                                componentVersions.put(implementationTitle, implementationVersion);
                            }
                        }
                    }
                } catch (IOException e) {
                    // Ignored
                }
            }
        }

        return componentVersions;
    }
}
