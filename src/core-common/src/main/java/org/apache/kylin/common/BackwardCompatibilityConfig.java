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

package org.apache.kylin.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import org.apache.kylin.common.util.OrderedProperties;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackwardCompatibilityConfig {

    private static final Logger logger = LoggerFactory.getLogger(BackwardCompatibilityConfig.class);

    private static final String KYLIN_BACKWARD_COMPATIBILITY = "kylin-backward-compatibility";
    private static final String PROP_FILE_POSTFIX = ".properties";

    private final Map<String, String> old2new = Maps.newConcurrentMap();
    private final Map<String, String> old2newPrefix = Maps.newConcurrentMap();

    public BackwardCompatibilityConfig() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        init(KYLIN_BACKWARD_COMPATIBILITY + PROP_FILE_POSTFIX, loader);
        for (int i = 0; i < 10; i++) {
            init(KYLIN_BACKWARD_COMPATIBILITY + (i) + PROP_FILE_POSTFIX, loader);
        }
    }

    private void init(String fileName, ClassLoader loader) {
        Properties props = new Properties();
        try (InputStream is = loader.getResourceAsStream(fileName)) {
            if (is == null) {
                return;
            }
            props.load(is);
        } catch (IOException e) {
            logger.error("Failed to get resource file<{}> ", fileName, e);
        }

        for (Entry<Object, Object> kv : props.entrySet()) {
            String key = (String) kv.getKey();
            String value = (String) kv.getValue();

            if (key.equals(value))
                continue; // no change

            if (value.contains(key))
                throw new IllegalStateException("New key '" + value + "' contains old key '" + key
                        + "' causes trouble to repeated find & replace");

            if (value.endsWith("."))
                old2newPrefix.put(key, value);
            else
                old2new.put(key, value);
        }
    }

    public String check(String key) {
        String newKey = old2new.get(key);
        if (newKey != null) {
            logger.warn("Config '{}' is deprecated, use '{}' instead", key, newKey);
            return newKey;
        }

        for (Entry<String, String> oldToNewPrefix : old2newPrefix.entrySet()) {
            String oldPrefix = oldToNewPrefix.getKey();
            if (key.startsWith(oldPrefix)) {
                String newPrefix = oldToNewPrefix.getValue();
                newKey = newPrefix + key.substring(oldPrefix.length());
                logger.warn("Config '{}' is deprecated, use '{}' instead", key, newKey);
                return newKey;
            }
        }

        return key;
    }

    public Map<String, String> check(Map<String, String> props) {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (Entry<String, String> kv : props.entrySet()) {
            result.put(check(kv.getKey()), kv.getValue());
        }
        return result;
    }

    public Properties check(Properties props) {
        Properties result = new Properties();
        for (Entry<Object, Object> kv : props.entrySet()) {
            result.setProperty(check((String) kv.getKey()), (String) kv.getValue());
        }
        return result;
    }

    public OrderedProperties check(OrderedProperties props) {
        OrderedProperties result = new OrderedProperties();
        for (Entry<String, String> kv : props.entrySet()) {
            result.setProperty(check(kv.getKey()), kv.getValue());
        }
        return result;
    }

    // ============================================================================

    public static void main(String[] args) throws IOException {
        String kylinRepoDir = args.length > 0 ? args[0] : ".";
        String outputDir = args.length > 1 ? args[1] : kylinRepoDir;
        generateFindAndReplaceScript(kylinRepoDir, outputDir);
    }

    private static void generateFindAndReplaceScript(String kylinRepoPath, String outputPath) throws IOException {
        BackwardCompatibilityConfig bcc = new BackwardCompatibilityConfig();
        File repoDir = new File(kylinRepoPath).getCanonicalFile();
        File outputDir = new File(outputPath).getCanonicalFile();

        // generate sed file
        File sedFile = new File(outputDir, "upgrade-old-config.sed");
        try (PrintWriter out = new PrintWriter(sedFile, Charset.defaultCharset().name())) {
            bcc.old2new.forEach((key, value) -> out.println("s/" + quote(key) + "/" + value + "/g"));
            bcc.old2newPrefix.forEach((key, value) -> out.println("s/" + quote(key) + "/" + value + "/g"));
        }

        // generate sh file
        File shFile = new File(outputDir, "upgrade-old-config.sh");
        try (PrintWriter out = new PrintWriter(shFile, Charset.defaultCharset().name())) {
            out.println("#!/bin/bash");
            Deque<File> stack = new ArrayDeque<>();
            stack.offerLast(repoDir);
            while (!stack.isEmpty()) {
                File dir = stack.pollLast();
                for (File f : Objects.requireNonNull(dir.listFiles())) {
                    if (f.getName().startsWith("."))
                        continue;
                    if (f.isDirectory()) {
                        if (acceptSourceDir(f))
                            stack.offerLast(f);
                    } else if (acceptSourceFile(f))
                        out.println("sed -i -f upgrade-old-config.sed " + f.getAbsolutePath());
                }
            }
        }

        System.out.println("Files generated:");
        System.out.println(shFile);
        System.out.println(sedFile);
    }

    private static String quote(String key) {
        return key.replace(".", "[.]");
    }

    private static boolean acceptSourceDir(File f) {
        // exclude webapp/app/components
        if (f.getName().equals("components") && f.getParentFile().getName().equals("app"))
            return false;
        else if (f.getName().equals("node_modules") && f.getParentFile().getName().equals("webapp"))
            return false;
        else
            return !f.getName().equals("target");
    }

    private static boolean acceptSourceFile(File f) {
        String name = f.getName();
        if (name.startsWith(KYLIN_BACKWARD_COMPATIBILITY))
            return false;
        else if (name.equals("KylinConfigTest.java"))
            return false;
        else if (name.endsWith("-site.xml"))
            return false;
        else
            return name.endsWith(".java") || name.endsWith(".js") || name.endsWith(".sh")
                    || name.endsWith(PROP_FILE_POSTFIX) || name.endsWith(".xml");
    }
}
