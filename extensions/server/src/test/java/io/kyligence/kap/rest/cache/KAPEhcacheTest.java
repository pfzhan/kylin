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

package io.kyligence.kap.rest.cache;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.junit.Ignore;
import org.junit.Test;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.ConfigurationFactory;

@Ignore
public class KAPEhcacheTest {

    @Test
    public void basicTest() throws InterruptedException, URISyntaxException, IOException {
        Log4jConfigurer.initLogger();

        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("ehcache-test.xml");
        URL resource2 = classLoader.getResource("kylin-log4j.properties");

        String x = FileUtils.readFileToString(new File(resource.toURI()));
        System.out.println(resource);
        System.out.println(resource2);
        System.out.println(x);

        Configuration conf = ConfigurationFactory.parseConfiguration(resource);
        CacheManager cacheManager = CacheManager.create(conf);

        //Create a Cache specifying its configuration.
        Cache testCache = cacheManager.getCache("SequenceSQLResults");

        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        byte[] blob = null;
        Random random = new Random();
        int msize = 40;

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }

        testCache.put(new Element("1", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("2", blob));
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("3", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("3") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * msize * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("4", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("3") == null);
        System.out.println(testCache.get("4") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println(testCache.getStatistics().getLocalDiskSizeInBytes());
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        System.gc();
        Thread.sleep(5000);
        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        cacheManager.shutdown();
    }
}
